"""Azure Service Bus configuration."""

from pydantic_settings import BaseSettings

from constants import AZURE_SERVICEBUS_CONNECTION_STRING, AZURE_SERVICEBUS_QUEUE_NAME, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER


class ServiceBusSettings(BaseSettings):
    """Azure Service Bus settings."""
    # Azure Service Bus
    connection_string: str = AZURE_SERVICEBUS_CONNECTION_STRING
    queue_name: str = AZURE_SERVICEBUS_QUEUE_NAME

    # Twilio settings
    twilio_account_sid: str = TWILIO_ACCOUNT_SID
    twilio_auth_token: str = TWILIO_AUTH_TOKEN
    twilio_phone_number: str = TWILIO_PHONE_NUMBER
