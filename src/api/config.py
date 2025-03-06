"""API configuration settings."""

from pydantic_settings import BaseSettings

from constants import (
    DATABASE_CONNECTION_STRING,
    LOGGING_LEVEL,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_PHONE_NUMBER,
)


class Settings(BaseSettings):
    """API settings configuration."""

    # API settings
    api_title: str = "Task Management API"
    api_version: str = "1.0.0"
    api_description: str = "API for task management and scheduling"

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000

    # Database settings
    database_url: str = DATABASE_CONNECTION_STRING

    # Logging
    logging_level: int = LOGGING_LEVEL

    # Twilio settings
    twilio_account_sid: str = TWILIO_ACCOUNT_SID
    twilio_auth_token: str = TWILIO_AUTH_TOKEN
    twilio_phone_number: str = TWILIO_PHONE_NUMBER


settings = Settings()
