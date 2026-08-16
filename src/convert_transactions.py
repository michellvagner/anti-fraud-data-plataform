from pathlib import Path
import polars as pl
import boto3

## %%

DATA_PATH = Path("data")
BUCKET_NAME = "anti-fraud-data-platform"
S3_PREFIX = "raw/transactions/"
TEMP_PATH = Path("temp")
TEMP_PATH.mkdir(exist_ok=True)

# %%

def convert_to_parquet(file_path: Path, output_path: Path):
    df = pl.read_csv(file_path, 
                    separator=";",
                    schema_overrides={
                        "TRANSACTION_ID": pl.String,
                        "BANK": pl.String,
                        "CARD_NUMBER": pl.String,
                        "AUTHORIZATION_CODE": pl.String,
                        "ACQUIRER_ID": pl.String,
                        "CURRENCY_CD": pl.String,
                        "TRANSACTION_COUNTRY_CD": pl.String,
                        "MERCHANT_ID": pl.String,
                        "REASON_CODE": pl.String,
                        "POS_NUMBER": pl.String,
                        "MERCHANT_CATEGORY_CODE": pl.String,
                        "PROCESS_CODE": pl.String,
                    }
                )

    df.write_parquet(output_path)

    return df


def upload_to_s3(file_path: Path, bucket_name: str, s3_key: str):
    s3 = boto3.client("s3")

    s3.upload_file(
        Filename=str(file_path),
        Bucket=bucket_name,
        Key=s3_key,
    )

# %%

def process_transactions():
    TEMP_PATH.mkdir(exist_ok=True)

    csv_files = list(DATA_PATH.glob("*.csv"))

    print(f"Arquivos encontrados: {len(csv_files)}")

    for index, file_path in enumerate(csv_files, start=1):
        try:
            print(f"\n[{index}/{len(csv_files)}] Processando: {file_path.name}")

            parquet_name = file_path.stem + ".parquet"
            output_path = TEMP_PATH / parquet_name

            df = convert_to_parquet(file_path, output_path)

            s3_key = f"{S3_PREFIX}{parquet_name}"

            upload_to_s3(
                file_path=output_path,
                bucket_name=BUCKET_NAME,
                s3_key=s3_key,
            )

            output_path.unlink()

            print(f"Upload realizado: {s3_key}")
            print(f"Linhas processadas: {df.height}")

        except Exception as e:
            print(f"ERRO ao processar {file_path.name}: {e}")

    if TEMP_PATH.exists() and not any(TEMP_PATH.iterdir()):
        TEMP_PATH.rmdir()
        print("Pasta temporária removida")

    print("\nProcessamento finalizado!")


if __name__ == "__main__":
    process_transactions()