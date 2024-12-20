from datetime import datetime
import logging
from typing import Iterator, Tuple, Any, Optional
from urllib.parse import unquote_plus as url_unquote_plus
import json

import boto3

from ..resource_object_key import ResourceObjectKey, ResourceObjectKeyType


logger = logging.getLogger(__name__)

SQSMessage = Any # type for the messages from SQS queue


class S3EventMessage:
    SUPPORTED_VERSION_MAJOR = 2
    SUPPORTED_VERSION_MINOR = 1
    OBJECT_CREATED_EVENT_NAME = "ObjectCreated:"
    OBJECT_REMOVED_EVENT_NAME = "ObjectRemoved:"
    EVENT_NAMES = (OBJECT_CREATED_EVENT_NAME, OBJECT_REMOVED_EVENT_NAME)

    def __init__(self, message: SQSMessage, record: dict):
        self._record = record
        self._message = message
        self._object_key = url_unquote_plus(record["s3"]["object"]["key"])
        self._object_key_parts = tuple(self._object_key.split("/"))
        self.resource_key = ResourceObjectKey.from_raw_key(self.object_key)
        if self.resource_key.ingestion_datetime is not None:
            self.time = self.resource_key.ingestion_datetime
        else:
            event_time = self._record["eventTime"]
            if event_time.endswith('Z'):
                # expand shorthand Z as python datetime can't parse it
                event_time = event_time[:-1] + '+00:00'
            self.time = datetime.fromisoformat(event_time)

    @classmethod
    def from_sqs_message(cls, bucket_name: str, message: SQSMessage):
        body = json.loads(message.body)
        if body.get("Event") == "s3:TestEvent":
            logger.debug("received an S3 test event message")
            return None

        for record in body["Records"]:
            try:
                version_major, version_minor = map(int, record["eventVersion"].split("."))
                if version_major > cls.SUPPORTED_VERSION_MAJOR or version_minor < cls.SUPPORTED_VERSION_MINOR:
                    logger.warning("received message with unsupported event version: %s", record["eventVersion"])
                    continue

                event_source, event_name = record["eventSource"], record["eventName"]
                event_bucket_name = record["s3"]["bucket"]["name"]

                if (
                    event_source == "aws:s3"
                    and event_name.startswith(cls.EVENT_NAMES)
                    and event_bucket_name == bucket_name
                ):
                    event_object_key = record["s3"]["object"]["key"]
                    logger.debug("sync event message for key %s", event_object_key)
                    is_folder_object = event_object_key.endswith('/')
                    yield None if is_folder_object else S3EventMessage(message, record)
            except (ValueError, KeyError, TypeError):
                logger.exception("unexpected schema")

    def mark_received(self):
        self._message.delete()

    def mark_invalid(self, message=None):
        logger.warning("cannot process event for object %s. event will be deleted, cause: %s", self.object_key, message)
        self._message.delete()

    def mark_error(self, error=None):
        # do nothing, after some retries the message will be delivered to the dead letter queue
        pass

    @property
    def type(self):
        return "created" if self.is_created_event() else "removed"

    def is_created_event(self):
        return self._record["eventName"].startswith(self.OBJECT_CREATED_EVENT_NAME)

    def is_removed_event(self):
        return self._record["eventName"].startswith(self.OBJECT_REMOVED_EVENT_NAME)

    def can_apply_to(self, resource: Optional[dict]):
        if self.resource_key.type == ResourceObjectKeyType.STREAMING:
            last_modified_iso = (resource or {}).get("last_modified")
            last_modified = (
                datetime.fromisoformat(last_modified_iso)
                if last_modified_iso is not None
                else datetime.fromtimestamp(0)
            )
            return last_modified < self.resource_key.ingestion_datetime
        else:
            sequencer = (resource or {}).get("aws_s3_sequencer", "0")
            return int(self.object_sequencer, 16) > int(sequencer, 16)

    @property
    def object_key(self) -> str:
        return self._object_key

    @property
    def object_key_parts(self) -> Tuple[str, ...]:
        return self._object_key_parts

    @property
    def object_key_prefixes(self) -> Tuple[str, ...]:
        return self._object_key_parts[:-1]

    @property
    def object_name(self) -> str:
        return self._object_key_parts[-1]

    @property
    def object_size(self) -> int:
        """Object size in bytes. Zero for removed events"""
        return self._record["s3"]["object"].get("size", 0)

    @property
    def object_sequencer(self) -> str:
        """A hexadecimal string that can be used to compare the order of two events for the same object key."""
        return self._record["s3"]["object"]["sequencer"]


def _poll_queue(queue_region: str, queue_url: str, driver_options) -> Iterator[SQSMessage]:
    queue = boto3.resource(
        "sqs",
        region_name=queue_region,
        aws_access_key_id=driver_options.get('key'),
        aws_secret_access_key=driver_options.get('secret'),
    ).Queue(queue_url)

    while True:
        messages = queue.receive_messages(MaxNumberOfMessages=10)
        if not messages:
            break
        yield from messages

def receive_s3_events(bucket_name: str, queue_region: str, queue_url: str, driver_options: dict) -> Iterator[S3EventMessage]:
    for message in _poll_queue(queue_region, queue_url, driver_options):
        logger.info("received message from sqs: %s", message)
        for event in S3EventMessage.from_sqs_message(bucket_name, message):
            if event is None:
                message.delete()
            else:
                yield event

# Fakes

def fake_receive_s3_events():
    class FakeSQSMessage:
        def __init__(self, *, delete=lambda: None, body={'Records': []}):
            self.body = body
            self.delete = delete

    from .fake_s3_event_messages import FAKE_MESSAGES

    yield from S3EventMessage.from_sqs_message('fake_bucket', FakeSQSMessage(body=FAKE_MESSAGES))
