"""Consumer for processing messages from Azure Service Bus."""

import json
from typing import Awaitable, Callable

from azure.servicebus.aio import ServiceBusClient
from twilio.rest import Client

from config import settings as api_settings
from config import settings
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger


class WhatsAppMessageConsumer:
    """Consumer for processing WhatsApp messages from Azure Service Bus."""

    def __init__(self, process_func: Callable[[WhatsAppQueueMessage], Awaitable[dict]]):
        """Initialize the consumer with settings and processing function."""
        self.process_func = process_func
        self.twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)

    async def process_messages(self, max_message_count: int = 10, max_wait_time: int = 5):
        """Process messages from the Azure Service Bus queue."""
        client = ServiceBusClient.from_connection_string(conn_str=settings.connection_string)

        async with client:
            receiver = client.get_queue_receiver(queue_name=settings.queue_name)
            async with receiver:
                messages = await receiver.receive_messages(max_message_count=max_message_count, max_wait_time=max_wait_time)

                for msg in messages:
                    try:
                        # Parse the message
                        data = json.loads(str(msg))
                        whatsapp_msg = WhatsAppQueueMessage.model_validate(data)

                        logger.info(f"Processing message: {whatsapp_msg.sms_message_sid}")

                        # Process the message - now sends WhatsApp message directly
                        result = await self.process_func(whatsapp_msg)

                        logger.info(f"Message processed with status: {result.get('status', 'unknown')}, SID: {result.get('message_sid', 'N/A')}")

                        # Complete the message
                        await receiver.complete_message(msg)
                    except Exception as e:
                        # Log the error and abandon the message
                        logger.error(f"Error processing message: {str(e)}")
                        await receiver.abandon_message(msg)
