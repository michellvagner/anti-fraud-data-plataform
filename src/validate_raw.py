import boto3
import polars as pl
from io import BytesIO

BUCKET_NAME = "anti-fraud-data-platform-vagner"
S3_KEY = "raw/transactions/transactions_2026_05_01.parquet"

s3 = boto3.client("s3")

response = s3.get_object(
    Bucket=BUCKET_NAME,
    Key=S3_KEY
)

parquet_file = BytesIO(response["Body"].read())

df = pl.read_parquet(parquet_file)

print(df.head())

print(f"\nQuantidade de linhas: {df.height}")
print(f"Quantidade de colunas: {df.width}")