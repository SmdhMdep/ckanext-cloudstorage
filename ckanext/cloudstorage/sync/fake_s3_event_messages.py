import json


PACKAGE_NAME = "sync-test-create-1"


def _event(name: str, object: dict) -> dict:
    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": "eu-west-1",
        "eventTime": "2023-09-09T09:09:09.900Z",
        "eventName": name,
        "userIdentity": {
            "principalId": "AWS:ABCDE1234:AWSFirehoseToS3DP"
        },
        "requestParameters": {
            "sourceIPAddress": "127.0.0.1"
        },
        "responseElements": {
            "x-amz-request-id": "520XRD4GEHACE3AH",
            "x-amz-id-2": "4AeTGBe9EF3EfBN2+BEAG32GA"
        },
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "AEPSyncEvent",
            "bucket": {
                "name": "fake_bucket",
                "ownerIdentity": {
                    "principalId": "ABCDE1234"
                },
                "arn": "arn:aws:s3:::fake_bucket"
            },
            "object": object
        }
    }

def _upload_created_event(name: str, sequence: int) -> dict:
    return _event("ObjectCreated:Put", {
        "key": f"1/test-organization/{PACKAGE_NAME}/{name}.txt",
        "size": 50,
        "sequencer": hex(sequence)[2:],
    })

def _upload_deleted_event(name: str, sequence: int) -> dict:
    return _event("ObjectRemoved:Delete", {
        "key": f"1/test-organization/{PACKAGE_NAME}/{name}.txt",
        "sequencer": hex(sequence)[2:],
    })

def _stream_created_event(name: str, date: str, sequence: int) -> dict:
    # date is of the format yyyy-mm-dd-hh-mm-ss
    return _event("ObjectCreated:Put", {
        "key": f"1/test-organization/{PACKAGE_NAME}/{name}/PUT-S3-Qj0zi-3-{date}-3d8d51f5-0fc4-3a21-8d2e-ff614b8e9a30",
        "size": 50,
        "sequencer": hex(sequence)[2:],
    })

def _stream_deleted_event(name: str, date: str, sequence: int) -> dict:
    # date is of the format yyyy-mm-dd-hh-mm-ss
    return _event("ObjectRemoved:Delete", {
        "key": f"1/test-organization/{PACKAGE_NAME}/{name}/PUT-S3-Qj0zi-3-{date}-3d8d51f5-0fc4-3a21-8d2e-ff614b8e9a30",
        "sequencer": hex(sequence)[2:],
    })


FAKE_MESSAGES = json.dumps({
    "Records": [
        _upload_created_event('study-data-from-event', 10),
        _upload_deleted_event('upload', 11),
        _stream_created_event('stream', '2023-07-24-09-07-42', 1),
        _stream_created_event('stream', '2023-09-29-09-07-42', 2),
    ]
})
