CREATE OR REPLACE STAGE ANTI_FRAUD_DB.RAW.STAGE_S3_TRANSACTIONS
    URL = 's3://anti-fraud-data-platform-vagner/raw/transactions/'
    CREDENTIALS = (
        AWS_KEY_ID = '{{ aws_key_id }}'
        AWS_SECRET_KEY = '{{ aws_secret_key }}'
        AWS_TOKEN = '{{ aws_token }}'
    )
    FILE_FORMAT = (
        TYPE = PARQUET
    );
