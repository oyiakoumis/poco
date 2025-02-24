"""API configuration settings."""

from pydantic_settings import BaseSettings

from constants import DATABASE_CONNECTION_STRING, LOGGING_LEVEL


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


settings = Settings()
