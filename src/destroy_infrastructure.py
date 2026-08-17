# %%
import subprocess
from pathlib import Path
from snowflake_connection import get_connection
# %%

sql_files = Path(__file__).parent.parent / "sql"

# %%
with open(sql_files / "9_suspend_drop_pipe_tasks.sql", "r", encoding="utf-8") as suspend_drop_pipe_tasks:
    queries = suspend_drop_pipe_tasks.read()

# %%

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
        conn.execute_string(queries)
        conn.close()

        print("🗑️   Snowflake infrastructure removida.")

    finally:
        conn.close()


if __name__ == "__main__":
    destroy_all()