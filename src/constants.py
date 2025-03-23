"""Constants for the application.

This module defines constants used throughout the application.
It uses AzureConfigProvider to retrieve values from Azure App Configuration
and Azure Key Vault, with fallback to environment variables.
"""

import logging
import os
from typing import Set

from dotenv import load_dotenv

from src.utils.azure_config import AzureConfigProvider

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
}

KEY_VAULT_SECRETS: Set[str] = {"DATABASE_CONNECTION_STRING", "DATABASE_NAME", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "OPENAI_API_KEY"}

# Initialize the Azure Configuration Provider
config = AzureConfigProvider(
    app_config_keys=APP_CONFIG_KEYS,
    key_vault_secrets=KEY_VAULT_SECRETS,
)

# App settings
LOGGING_LEVEL = getattr(logging, config.get_config("LOGGING_LEVEL", "DEBUG").upper(), logging.DEBUG)
IS_LOCAL = config.is_local
ENVIRONMENT = config.get_config("ENVIRONMENT")

# OpenAI settings
OPENAI_API_URL = config.get_config("AZURE_OPENAI_ENDPOINT")
OPENAI_API_KEY = config.get_secret("OPENAI_API_KEY")

# MongoDB settings
DATABASE_CONNECTION_STRING = config.get_secret("DATABASE_CONNECTION_STRING")
DATABASE_NAME = config.get_secret("DATABASE_NAME")

# Twilio settings
API_URL = config.get_config("API_URL")
TWILIO_ACCOUNT_SID = config.get_secret("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = config.get_secret("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = config.get_secret("TWILIO_PHONE_NUMBER")

# Azure Blob Storage settings
AZURE_STORAGE_ACCOUNT = config.get_secret("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_CONTAINER = config.get_secret("AZURE_STORAGE_CONTAINER")
