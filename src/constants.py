import logging
import os

from dotenv import load_dotenv

load_dotenv()


def parse_boolean(value, default=False):
    """Parse a string value into a boolean."""
    if value is None:
        return default

    value = str(value).lower().strip()

    if value in ("true", "yes", "1", "t", "y"):
        return True
    elif value in ("false", "no", "0", "f", "n"):
        return False

    return default


# App settings
LOGGING_LEVEL = getattr(logging, os.environ.get("LOGGING_LEVEL", "DEBUG").upper(), logging.DEBUG)
IS_LOCAL = parse_boolean(os.environ.get("IS_LOCAL"), default=False)

# MongoDB settings
DATABASE_CONNECTION_STRING = os.environ["DATABASE_CONNECTION_STRING"]

# Twilio settings
API_URL = os.environ["API_URL"]
TWILIO_ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = os.environ["TWILIO_PHONE_NUMBER"]

# Azure Blob Storage settings
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_STORAGE_ACCOUNT_KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
AZURE_BLOB_CONTAINER_NAME = os.environ["AZURE_BLOB_CONTAINER_NAME"]

# Gemini
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
