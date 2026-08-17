# %%
import boto3
import subprocess
from pathlib import Path
from execute_sql import setup_snowflake
from upload_transactions import upload_transactions
from convert_transactions import process_transactions

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

    print("\n" + "=" * 50)
    print("PIPELINE FINALIZADO COM SUCESSO!")
    print("=" * 50)

if __name__ == "__main__":
    main()
# %%
