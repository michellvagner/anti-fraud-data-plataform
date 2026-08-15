from pathlib import Path
import polars as pl
import boto3

## %%

s3 = boto3.client("s3")

DATA_PATH = Path("data")
BUCKET_NAME = "anti-fraud-data-platform-vagner"
S3_PREFIX = "raw/transactions/"
TEMP_PATH = Path("temp")
TEMP_PATH.mkdir(exist_ok=True)

# %%

csv_files = list(DATA_PATH.glob("*.csv"))

print(f"Arquivos encontrados: {len(csv_files)}")

# %%

for index, file_path in enumerate(csv_files, start=1):

    try:

        print(f"\n[{index}/{len(csv_files)}] Processando: {file_path.name}")

        df = pl.read_csv(file_path, separator=";")

        parquet_name = file_path.stem + ".parquet"

        output_path = TEMP_PATH / parquet_name

        output_path.parent.mkdir(exist_ok=True)

        df.write_parquet(output_path)

        s3_key = f"{S3_PREFIX}{parquet_name}"

        s3.upload_file(
            Filename=str(output_path),
            Bucket=BUCKET_NAME,
            Key=s3_key
        )

        output_path.unlink()
        print(f"Upload realizado: {s3_key}")

    except Exception as e:
        print(f"ERRO ao processar {file_path.name}: {e}")

if TEMP_PATH.exists() and not any(TEMP_PATH.iterdir()):
    TEMP_PATH.rmdir()
    print("Pasta temporária removida")

print("\nProcessamento finalizado!")