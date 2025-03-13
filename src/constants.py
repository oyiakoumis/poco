import json
import logging
import os

from dotenv import load_dotenv

load_dotenv()


DATABASE_CONNECTION_STRING = os.environ["DATABASE_CONNECTION_STRING"]
LOGGING_LEVEL = logging.DEBUG

# Twilio settings
API_URL = os.environ["API_URL"]
TWILIO_ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = os.environ["TWILIO_PHONE_NUMBER"]

# Azure Blob Storage settings
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_STORAGE_ACCOUNT_KEY = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
AZURE_BLOB_CONTAINER_NAME = os.environ["AZURE_BLOB_CONTAINER_NAME"]
