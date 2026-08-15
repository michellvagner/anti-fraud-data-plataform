import subprocess
from pathlib import Path
from snowflake_connection import get_connection


def destroy_all():
    conn = get_connection()

    terraform_directory = (
        Path(__file__).resolve().parent.parent
        / "infrastructure"
    )

    subprocess.run(
        ["terraform", "destroy", "-auto-approve"],
        cwd=terraform_directory,
        check=True,
    )

    try:
        conn.execute_string("""
            DROP DATABASE IF EXISTS ANTI_FRAUD_DB;
        """)

        print("🗑️   Snowflake infrastructure removida.")

    finally:
        conn.close()


if __name__ == "__main__":
    destroy_all()