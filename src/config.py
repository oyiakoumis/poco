"""Azure Service Bus and Blob Storage configuration."""

from pydantic_settings import BaseSettings

from constants import (
    API_URL,
    AZURE_BLOB_CONTAINER_NAME,
    AZURE_SERVICEBUS_CONNECTION_STRING,
    AZURE_SERVICEBUS_QUEUE_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_CONNECTION_STRING,
    DATABASE_CONNECTION_STRING,
    LOGGING_LEVEL,
    REDIS_HOSTS,
    REDIS_LOCK_TIMEOUT_MS,
    REDIS_PASSWORDS,
    REDIS_PORT,
    REDIS_USERNAME,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_PHONE_NUMBER,
)


class Settings(BaseSettings):
    """Azure Service Bus settings."""

    # API settings
    api_title: str = "Task Management API"
    api_version: str = "1.0.0"
    api_description: str = "API for task management and scheduling"
    host: str = "0.0.0.0"
    port: int = 8000

    # Database settings
    database_url: str = DATABASE_CONNECTION_STRING

    # Logging
    logging_level: int = LOGGING_LEVEL

    # Azure Service Bus
    connection_string: str = AZURE_SERVICEBUS_CONNECTION_STRING
    queue_name: str = AZURE_SERVICEBUS_QUEUE_NAME

    # Twilio settings
    api_url: str = API_URL
    twilio_account_sid: str = TWILIO_ACCOUNT_SID
    twilio_auth_token: str = TWILIO_AUTH_TOKEN
    twilio_phone_number: str = TWILIO_PHONE_NUMBER

    # Azure Blob Storage settings
    azure_storage_connection_string: str = AZURE_STORAGE_CONNECTION_STRING
    azure_storage_account_key: str = AZURE_STORAGE_ACCOUNT_KEY
    azure_blob_container_name: str = AZURE_BLOB_CONTAINER_NAME

    # Redis settings
    redis_hosts: list = REDIS_HOSTS
    redis_username: str = REDIS_USERNAME
    redis_passwords: str = REDIS_PASSWORDS
    redis_port: int = REDIS_PORT
    redis_lock_timeout_ms: int = REDIS_LOCK_TIMEOUT_MS


settings = Settings()
