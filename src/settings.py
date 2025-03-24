"""App settings."""

from constants import (
    API_URL,
    AZURE_STORAGE_ACCOUNT,
    AZURE_STORAGE_CONTAINER,
    DATABASE_CONNECTION_STRING,
    DATABASE_NAME,
    ENVIRONMENT,
    IS_LOCAL,
    OPENAI_API_KEY,
    OPENAI_API_URL,
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
    environment: str = ENVIRONMENT

    # Database settings
    database_connection_string: str = DATABASE_CONNECTION_STRING
    database_name: str = DATABASE_NAME

    # OpenAI settings
    openai_api_url: str = OPENAI_API_URL
    open_api_key: str = OPENAI_API_KEY

    # Twilio settings
    api_url: str = API_URL
    twilio_account_sid: str = TWILIO_ACCOUNT_SID
    twilio_auth_token: str = TWILIO_AUTH_TOKEN
    twilio_phone_number: str = TWILIO_PHONE_NUMBER
    twilio_max_message_length: int = 1600

    # Azure Blob Storage settings
    azure_storage_account: str = AZURE_STORAGE_ACCOUNT
    azure_storage_container: str = AZURE_STORAGE_CONTAINER


settings = Settings()
