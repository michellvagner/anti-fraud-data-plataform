import configparser
from pathlib import Path


def get_aws_credentials(profile="default"):
    credentials_path = Path.home() / ".aws" / "credentials"

    config = configparser.ConfigParser()
    config.read(credentials_path)

    return {
        "aws_key_id": config[profile]["aws_access_key_id"],
        "aws_secret_key": config[profile]["aws_secret_access_key"],
        "aws_token": config[profile].get("aws_session_token"),
    }