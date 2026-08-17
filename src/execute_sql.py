# %%
import json
from pathlib import Path
from jinja2 import Template
from snowflake_connection import get_connection
from aws_credentials import get_aws_credentials
from aws_s3 import configure_s3_notification


# %%

def get_pipe_sqs_arn(conn):
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT SYSTEM$PIPE_STATUS(
                'ANTI_FRAUD_DB.RAW.BRONZE_TRANSACTIONS_PIPE'
            )
        """)

        result = cursor.fetchone()[0]

        pipe_status = json.loads(result)

        sqs_arn = pipe_status["notificationChannelName"]

        return sqs_arn

    finally:
        cursor.close()

# %%

def execute_sql_files(conn, start_file, end_file):
    _ = conn.cursor()
    sql_directory = Path(__file__).parent.parent / "sql"

    sql_files = sorted(sql_directory.glob("*.sql"))

    for sql_file in sql_files:
        file_number = int(sql_file.name.split("_")[0])

        if not start_file <= file_number <= end_file:
            continue

        print(f"Executando: {sql_file.name}")

        sql = sql_file.read_text(encoding="utf-8")

        if sql_file.name == "1_stage_s3_transactions.sql":
            template = Template(sql)
            sql = template.render(**get_aws_credentials())

        _.execute("USE DATABASE ANTI_FRAUD_DB")
        _.execute("USE SCHEMA RAW")  

        cursors = conn.execute_string(sql)

        for cursor in cursors:
            cursor.close()

        print(f"✅ Concluído: {sql_file.name}")

# %%

def setup_snowflake():

    conn = get_connection()

    try:
        execute_sql_files(conn, start_file=0, end_file=5)
        sqs_arn = get_pipe_sqs_arn(conn)

        configure_s3_notification(sqs_arn)

        execute_sql_files(conn, start_file=6, end_file=8)

    finally:
        conn.close()
        print("\nConexão encerrada.")

if __name__ == "__main__":
    setup_snowflake()
