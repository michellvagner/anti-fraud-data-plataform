# %%
import time
import boto3
import subprocess
from pathlib import Path
from execute_sql import setup_snowflake
from upload_transactions import upload_transactions
from snowflake_connection import get_connection

# %%

def main():
    print("=" * 50)
    print("INICIANDO PIPELINE")
    print("=" * 50)

    print("\n[1/4] Criando infraestrutura Terraform...")
    terraform_directory = (
        Path(__file__).resolve().parent.parent
        / "infrastructure"
    )

    subprocess.run(
        ["terraform", "apply", "-auto-approve"],
        cwd=terraform_directory,
        check=True,
    )

    print("\n[2/4] Criando infraestrutura Snowflake...")
    setup_snowflake()

    print("\n[3/4] Enviando transações para S3...")
    upload_transactions()

    print("\n[4/4] Convertendo transações...")

    glue = boto3.client("glue")

    response = glue.start_job_run(
        JobName="anti-fraud-csv-to-parquet"
    )
    
    job_run_id = response["JobRunId"]
    print(f"Glue Job iniciado: {job_run_id}")

    while True:
        job = glue.get_job_run(
            JobName="anti-fraud-csv-to-parquet",
            RunId=job_run_id
        )

        status = job["JobRun"]["JobRunState"]

        print(f"Status do Glue: {status}")

        if status == "SUCCEEDED":
            print("Glue finalizado com sucesso!")
            break

        elif status in ["FAILED", "STOPPED", "TIMEOUT", "ERROR"]:
            raise Exception(f"Glue Job terminou com status: {status}")

        time.sleep(50)

    conn = get_connection()

    cursor = conn.cursor()

    time.sleep(10)

    cursor.execute("USE DATABASE ANTI_FRAUD_DB")
    cursor.execute("USE SCHEMA RAW")

    cursor.execute("""
        ALTER PIPE BRONZE_TRANSACTIONS_PIPE REFRESH
    """)

    cursor.close()

    print("\n" + "=" * 50)
    print("PIPELINE FINALIZADO COM SUCESSO!")
    print("=" * 50)

if __name__ == "__main__":
    main()
# %%
