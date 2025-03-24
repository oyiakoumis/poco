"""Constants for the application."""

import logging
import os
from typing import Set

from dotenv import load_dotenv

from utils.azure_config import AzureConfigProvider

# Load environment variables from .env file
load_dotenv()


# Define configuration keys and secrets
APP_CONFIG_KEYS: Set[str] = {
    "LOGGING_LEVEL",
    "IS_LOCAL",
    "ENVIRONMENT",
    "API_URL",
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_CONTAINER",
    "TWILIO_PHONE_NUMBER",
    "AZURE_OPENAI_ENDPOINT",
    "DATABASE_NAME",
}

KEY_VAULT_SECRETS: Set[str] = {"mongodb-atlas-connection-string", "twilio-account-sid", "twilio-auth-token", "openai-api-key"}

# Initialize the Azure Configuration Provider
config = AzureConfigProvider(
    app_config_keys=APP_CONFIG_KEYS,
    key_vault_secrets=KEY_VAULT_SECRETS,
)

# App settings
IS_LOCAL = config.is_local
ENVIRONMENT = config.get_config("ENVIRONMENT")

# OpenAI settings
OPENAI_API_URL = config.get_config("AZURE_OPENAI_ENDPOINT")
OPENAI_API_KEY = config.get_secret("openai-api-key")

# MongoDB settings
DATABASE_CONNECTION_STRING = config.get_secret("mongodb-atlas-connection-string")
DATABASE_NAME = config.get_config("DATABASE_NAME")

# Twilio settings
API_URL = config.get_config("API_URL") if not IS_LOCAL else os.environ["API_URL"]
TWILIO_ACCOUNT_SID = config.get_secret("twilio-account-sid")
TWILIO_AUTH_TOKEN = config.get_secret("twilio-auth-token")
TWILIO_PHONE_NUMBER = config.get_config("TWILIO_PHONE_NUMBER")

# Azure Blob Storage settings
AZURE_STORAGE_ACCOUNT = config.get_config("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_CONTAINER = config.get_config("AZURE_STORAGE_CONTAINER")
