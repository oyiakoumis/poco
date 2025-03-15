"""App settings."""

from constants import (
    API_URL,
    AZURE_BLOB_CONTAINER_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_CONNECTION_STRING,
    DATABASE_CONNECTION_STRING,
    IS_LOCAL,
    LOGGING_LEVEL,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_PHONE_NUMBER,
)


class Settings:
    # API settings
    api_title: str = "Task Management API"
    api_version: str = "1.0.0"
    api_description: str = "API for task management and scheduling"
    host: str = "0.0.0.0"
    port: int = 8000
    is_local: bool = IS_LOCAL

    # Database settings
    database_connection_string: str = DATABASE_CONNECTION_STRING

    # Logging
    logging_level: int = LOGGING_LEVEL

    # Twilio settings
    api_url: str = API_URL
    twilio_account_sid: str = TWILIO_ACCOUNT_SID
    twilio_auth_token: str = TWILIO_AUTH_TOKEN
    twilio_phone_number: str = TWILIO_PHONE_NUMBER
    twilio_max_message_length: int = 1600

    # Azure Blob Storage settings
    azure_storage_connection_string: str = AZURE_STORAGE_CONNECTION_STRING
    azure_storage_account_key: str = AZURE_STORAGE_ACCOUNT_KEY
    azure_blob_container_name: str = AZURE_BLOB_CONTAINER_NAME


settings = Settings()
