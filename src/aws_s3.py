import boto3


def configure_s3_notification(sqs_arn: str):
    s3 = boto3.client("s3")

    bucket_name = "anti-fraud-data-platform-vagner"

    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": sqs_arn,
                    "Events": [
                        "s3:ObjectCreated:*"
                    ],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {
                                    "Name": "prefix",
                                    "Value": "raw/transactions/"
                                }
                            ]
                        }
                    }
                }
            ]
        }
    )

    print("✅ Notificação S3 → SQS configurada!")