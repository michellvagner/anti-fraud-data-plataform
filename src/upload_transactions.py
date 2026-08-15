from pathlib import Path
import boto3

# %%

BUCKET_NAME = "anti-fraud-data-platform-vagner"
DATA_PATH = Path("data")
S3_PREFIX = "landing/transactions/"

# %%

s3 = boto3.client("s3")

csv_files = list(DATA_PATH.glob("*.csv"))

# %%

total_files = len(csv_files)

for index, file in enumerate(csv_files, start=1):
    s3_key = f"{S3_PREFIX}{file.name}"

    print(f"[{index}/{total_files}] Enviando: {file.name}")

    s3.upload_file(
        Filename=str(file),
        Bucket=BUCKET_NAME,
        Key=s3_key
    )

print(f"Todos os arquivos foram enviados com sucesso!\n")