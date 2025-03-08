"""Azure Service Bus configuration."""

from pydantic_settings import BaseSettings

from constants import AZURE_SERVICEBUS_CONNECTION_STRING, AZURE_SERVICEBUS_QUEUE_NAME


class ServiceBusSettings(BaseSettings):
    """Azure Service Bus settings."""

    connection_string: str = AZURE_SERVICEBUS_CONNECTION_STRING
    queue_name: str = AZURE_SERVICEBUS_QUEUE_NAME
