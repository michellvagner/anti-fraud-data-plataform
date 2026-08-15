# %%
import os
import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


def get_connection():

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="LAB_WH",
        role="ACCOUNTADMIN"
    )


if __name__ == "__main__":
    get_connection()
# %%
