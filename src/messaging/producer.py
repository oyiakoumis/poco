"""Producer for sending messages to Azure Service Bus."""

import json

from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient

from messaging.config import ServiceBusSettings
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger


class WhatsAppMessageProducer:
    """Producer for sending WhatsApp messages to Azure Service Bus."""

    def __init__(self, settings: ServiceBusSettings = None):
        """Initialize the producer with settings."""
        self.settings = settings or ServiceBusSettings()
        self.client = ServiceBusClient.from_connection_string(conn_str=self.settings.connection_string)

    async def send_message(self, message: WhatsAppQueueMessage):
        """Send a message to the Azure Service Bus queue."""
        logger.info(f"Sending message to queue: {self.settings.queue_name}")

        async with self.client:
            sender = self.client.get_queue_sender(queue_name=self.settings.queue_name)
            message_json = json.dumps(message.model_dump())
            sb_message = ServiceBusMessage(message_json)

            try:
                await sender.send_messages(sb_message)
                logger.info(f"Message sent to queue: {message.sms_message_sid}")
            except Exception as e:
                logger.error(f"Error sending message to queue: {str(e)}")
                raise
