'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import json
import uuid
from dateutil.parser import parse
import logging
import boto3
import os
import traceback
from botocore.exceptions import ClientError
from google.cloud import pubsub_v1
from google.api_core import retry
from google.oauth2 import service_account

# Logging options
logging.basicConfig()
logger = logging.getLogger()
if 'LOG_LEVEL' in os.environ:
    logger.setLevel(os.environ["LOG_LEVEL"])
    logger.info(f"Log level set to {logger.getEffectiveLevel()}")
else:
    logger.setLevel(logging.INFO)

# GCP Pub/Sub options
GCP_CREDENTIALS = os.environ["GCP_CREDENTIALS"]
GCP_PROJECT_NAME = os.environ["GCP_PROJECT_NAME"]
GCP_SUBSCRIPTION_NAME = os.environ["GCP_SUBSCRIPTION_NAME"]
MAX_MESSAGES_PER_READ = os.environ.get("MAX_MESSAGES_PER_READ", 500)
GCP_SUBSCRIPTION_READ_TIMEOUT = os.environ.get("GCP_SUBSCRIPTION_READ_TIMEOUT", 10)

# CloudTrail Lake options
CLOUDTRAIL_LAKE_CHANNEL_ARN = os.environ["CLOUDTRAIL_LAKE_CHANNEL_ARN"]
MAX_AUDIT_EVENTS_PER_PUT = 100 # Current limit is 100 or 1 MB - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudtrail-data/client/put_audit_events.html
DEFAULT_USER_TYPE = os.environ.get("DEFAULT_USER_TYPE", "GoogleCloudUser")
FAILED_INGESTION_EVENTS_DLQ = os.environ.get("FAILED_INGESTION_EVENTS_DLQ")

# Boto3 resources
session = boto3.Session()
sts_client = session.client('sts')
cloudtrail_client = session.client('cloudtrail-data')
sqs_client = session.client('sqs')
RECIPIENT_ACCOUNT_ID = sts_client.get_caller_identity()['Account']


def transform_entry(entry: str) -> dict or None:
    try:
        entry = json.loads(entry)
    except Exception as err:
        raise err
    logger.debug(f"Transforming entry: {entry}")

    # Skip if it's not a Google Cloud Audit Log type
    if "protoPayload" not in entry or entry["protoPayload"].get("@type") != "type.googleapis.com/google.cloud.audit.AuditLog":
        logger.info(f"Skipping non-audit log entry: {entry}")
        return None

    uid = entry.get("insertId", str(uuid.uuid4()))
    event_version = entry.get("receiveTimestamp") or entry.get("timestamp")
    user_type = DEFAULT_USER_TYPE
    user_id = entry["protoPayload"].get("authenticationInfo", {'principalEmail': 'None'}).get("principalEmail")
    user_details = entry["protoPayload"].get("authorizationInfo", "None")
    event_source = entry["protoPayload"].get("serviceName", "None")
    event_name = entry["protoPayload"].get("methodName", "None")
    event_time = entry.get("timestamp")
    user_agent = "None"
    source_ip = "None"
    if entry["protoPayload"].get("requestMetadata") is not None:
        user_agent = entry["protoPayload"]["requestMetadata"]["callerSuppliedUserAgent"] if \
            entry["protoPayload"]["requestMetadata"].get("callerSuppliedUserAgent") is not None else "None"
        source_ip = entry["protoPayload"]["requestMetadata"]["callerIp"] if \
            entry["protoPayload"]["requestMetadata"].get("callerIp") is not None else "None"
    request_params = entry["protoPayload"].get("request", {})
    response_elements = entry["protoPayload"].get("response", {})
    resource = entry.get("resource", "None")

    event = {
        "version": event_version,
        "userIdentity": {
            "type": user_type,
            "principalId": user_id,
            "details": user_details[0] if user_details and len(user_details) > 0 else "None"
        },
        "userAgent": user_agent,
        "eventSource": event_source,
        "eventName": event_name,
        "eventTime": parse(event_time).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "UID": uid,
        "requestParameters": request_params,
        "responseElements": response_elements,
        "sourceIPAddress": source_ip,
        "recipientAccountId": RECIPIENT_ACCOUNT_ID,
        "additionalEventData": resource
    }
    logger.debug(f"Transformed entry: {event}")

    audit_event = {
        "eventData": json.dumps(event),
        "id": uid
    }
    return audit_event


def read_gcp_messages(subscriber: pubsub_v1.SubscriberClient) -> list:
    subscription_path = subscriber.subscription_path(GCP_PROJECT_NAME, GCP_SUBSCRIPTION_NAME)
    messages = list()

    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": int(MAX_MESSAGES_PER_READ)},
        retry=retry.Retry(timeout=int(GCP_SUBSCRIPTION_READ_TIMEOUT)),
    )
    if len(response.received_messages) == 0:
        return list()

    for received_message in response.received_messages:
        message_data_str = received_message.message.data.decode('utf-8')
        logger.debug(f"Received message from topic: {message_data_str}")
        messages.append({'ack_id': received_message.ack_id, 'message': message_data_str})

    logger.debug(f"Received {len(response.received_messages)} messages from {subscription_path}")

    return messages


def ack_gcp_messages(ack_ids: list, subscriber: pubsub_v1.SubscriberClient) -> bool:
    subscription_path = subscriber.subscription_path(GCP_PROJECT_NAME, GCP_SUBSCRIPTION_NAME)
    try:
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
        return True

    except Exception as err:
        logger.error(f"Failed to acknowledge GCP messages {ack_ids}")
        raise err


def ingest_audit_events(events: list) -> dict:
    try:
        output = cloudtrail_client.put_audit_events(
            auditEvents=events,
            channelArn=CLOUDTRAIL_LAKE_CHANNEL_ARN)
        success = True
        if len(output['failed']) > 0:
            success = False
        return {'result': success, 'failed': output['failed'], 'successful': output['successful']}

    except ClientError as err:
        logger.error(f"Failed to put events: {traceback.format_exc()}")
        raise err


def send_to_queue(payload: dict, unique_id: str) -> bool:
    try:
        sqs_response = sqs_client.send_message(
            QueueUrl=FAILED_INGESTION_EVENTS_DLQ,
            MessageBody=json.dumps(payload),
            MessageDeduplicationId=unique_id,
            MessageGroupId="failed_ingestion_events"
            )
        logger.warning(f"Failed ingestion event sent to DQL: {sqs_response}")
    except Exception as err:
        logger.error(f"Failed to send to DLQ: {err}")
        raise err
    return True


def lambda_handler(event, context) -> None:
    logger.info("Starting GCP Cloud Audit Logging Ingestion")
    logger.info(f"GCP Project: {GCP_PROJECT_NAME}")
    logger.info(f"GCP Subscription: {GCP_SUBSCRIPTION_NAME}")
    logger.info(f"CloudTrail Lake Channel Arn: {CLOUDTRAIL_LAKE_CHANNEL_ARN}")
    # Use GCP credentials in environment variable to build a GCP client
    credentials = service_account.Credentials.from_service_account_info(json.loads(GCP_CREDENTIALS))
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    with subscriber:
        # Read audit events from GCP Pub/Sub subscription
        messages = read_gcp_messages(subscriber)
        logger.info(f"Read {len(messages)} messages from {GCP_PROJECT_NAME} / {GCP_SUBSCRIPTION_NAME}")

        # If no audit events were found, quit
        if len(messages) == 0:
            logger.info(f"No audit events were found to ingest. Quitting.")
            return None

        # Batch audit events to reduce API calls to CloudTrail Lake; current limit per batch is 100 events or 1 MB
        iter_count = 0
        total_audit_events = 0

        # Make a map of message ids to messages to reference later in the while loop
        message_dict = dict()
        for msg in messages:
            msg_id = msg['message']['id']
            msg_event = msg['message']
            message_dict[msg_id] = msg_event

        while len(messages) > 0:
            iter_count += 1
            remainder = list()
            logger.info(f"Audit events batch #{iter_count}. {len(messages)} entries remaining.")
            if len(messages) > MAX_AUDIT_EVENTS_PER_PUT:
                remainder = messages[MAX_AUDIT_EVENTS_PER_PUT:]
                messages = messages[:MAX_AUDIT_EVENTS_PER_PUT]
            audit_events = list()

            # Transform each event so it can be ingested to CloudTrail
            for entry in messages:                
                try:
                    audit_event = transform_entry(entry['message'])
                except:                    
                    audit_event = None
                if audit_event is None:
                    logger.error(f"Skipping entry because transform failed: {entry}")
                    continue
                entry['message'] = audit_event
                audit_events.append(entry)

            try:
                logger.info(f"Ingesting {len(audit_events)} audit events into CloudTrail Lake channel [{CLOUDTRAIL_LAKE_CHANNEL_ARN}]: {audit_events}")

                # Ingest the batch of events to CloudTrail
                ingest_result = ingest_audit_events([m['message'] for m in audit_events])

                # Send failed events to SQS queue
                if not ingest_result['result']:
                    logger.warning(f"The following audit events were not ingested successfully: {ingest_result['failed']}")
                    if FAILED_INGESTION_EVENTS_DLQ and len(ingest_result['failed']) > 0:
                        failed_ids = [e['id'] for e in ingest_result['failed']]
                        for failed_id in failed_ids:
                            send_to_queue(message_dict[failed_id], failed_id)

                total_audit_events += len(ingest_result['successful'])
                logger.info(f"{len(ingest_result['successful'])} audit events were successfully ingested")

                # Acknowledge the messages on the GCP subscription so that they are no longer distributed
                logger.info("Acknowledging GCP messages")
                ack_success = ack_gcp_messages([m['ack_id'] for m in audit_events], subscriber)
                logger.info(f"Messages were {'not ' if not ack_success else ''}all successfully acknowledged")

            except:
                logger.error(f"Error ingesting audit events: {traceback.format_exc()}")

            messages = list(remainder)

    logger.info(f"Ingested total {total_audit_events} audit events successfully")
