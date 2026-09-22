"""Provision disposable local broker identities and queues on each startup."""

import json
import os
from pathlib import Path
import shlex

import boto3
from botocore.exceptions import ClientError


REGION = "us-east-1"
ACCOUNT = "000000000000"
session = boto3.Session(
    aws_access_key_id="test", aws_secret_access_key="test", region_name=REGION
)
sqs = session.client("sqs", endpoint_url="http://localhost:4566")
iam = session.client("iam", endpoint_url="http://localhost:4566")
base = Path("/opt/wager-bootstrap")
credentials = Path("/run/wager-aws")
credentials.mkdir(parents=True, exist_ok=True)


def queue(name, attributes):
    url = sqs.create_queue(
        QueueName=name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
            "MessageRetentionPeriod": "345600",
            **attributes,
        },
    )["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    return url, arn


dlq_url, dlq_arn = queue(
    "wager-transactions-dlq.fifo", {"MessageRetentionPeriod": "1209600"}
)
request_url, request_arn = queue(
    "wager-transactions.fifo",
    {
        "VisibilityTimeout": "30",
        "ReceiveMessageWaitTimeSeconds": "10",
        "RedrivePolicy": json.dumps(
            {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}
        ),
    },
)
event_url, event_arn = queue("wager-events.fifo", {"VisibilityTimeout": "30"})
sqs.set_queue_attributes(
    QueueUrl=dlq_url,
    Attributes={
        "RedriveAllowPolicy": json.dumps(
            {"redrivePermission": "byQueue", "sourceQueueArns": [request_arn]}
        )
    },
)

# Identity policies separate trusted ingress, the financial worker and event readers.
for short_name in ("app", "ingress", "events"):
    user = "wager-" + short_name
    try:
        iam.create_user(UserName=user)
    except ClientError as error:
        if error.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
    policy = (base / "policies" / (short_name + ".json")).read_text()
    iam.put_user_policy(UserName=user, PolicyName="wager-scoped-access", PolicyDocument=policy)
    # Rotate disposable credentials when the emulator restarts; never print them to logs.
    for key in iam.list_access_keys(UserName=user)["AccessKeyMetadata"]:
        iam.delete_access_key(UserName=user, AccessKeyId=key["AccessKeyId"])
    key = iam.create_access_key(UserName=user)["AccessKey"]
    path = credentials / (short_name + ".env")
    path.write_text(
        "AWS_ACCESS_KEY_ID=" + shlex.quote(key["AccessKeyId"]) + "\n"
        "AWS_SECRET_ACCESS_KEY=" + shlex.quote(key["SecretAccessKey"]) + "\n"
        "AWS_REGION=" + REGION + "\n"
    )
    # The application can read only its own identity, even though the volume also
    # contains root-readable credentials used by local operator helper scripts.
    owner = 10001 if short_name == "app" else 0
    os.chown(path, owner, owner)
    path.chmod(0o400)


def queue_policy(url, statements):
    sqs.set_queue_attributes(
        QueueUrl=url,
        Attributes={"Policy": json.dumps({"Version": "2012-10-17", "Statement": statements})},
    )


def allow(user, actions, resource):
    return {
        "Effect": "Allow",
        "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:user/wager-{user}"},
        "Action": actions,
        "Resource": resource,
    }


queue_policy(
    request_url,
    [
        allow("ingress", ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"], request_arn),
        allow("app", ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"], request_arn),
    ],
)
queue_policy(
    event_url,
    [
        allow("app", ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"], event_arn),
        allow("events", ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"], event_arn),
    ],
)
print("Provisioned request queue, DLQ, event queue and three scoped IAM identities.")
