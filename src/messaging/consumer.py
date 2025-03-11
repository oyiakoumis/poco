"""Consumer for processing messages from Azure Service Bus."""

import asyncio
import json
from typing import Awaitable, Callable

from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver
from azure.servicebus import NEXT_AVAILABLE_SESSION
from twilio.rest import Client

from config import settings
from config import settings as api_settings
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger


class WhatsAppMessageConsumer:
    """Consumer for processing WhatsApp messages from Azure Service Bus."""

    def __init__(self, process_func: Callable[[WhatsAppQueueMessage], Awaitable[dict]]):
        """Initialize the consumer with settings and processing function."""
        self.process_func = process_func
        self.twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)
        self.client = ServiceBusClient.from_connection_string(conn_str=settings.connection_string)

    async def process_messages(self, max_message_count: int = 20, max_wait_time: int = 0.1):
        """Process messages from the Azure Service Bus queue using sessions."""
        try:
            # Try to get a session receiver
            async with self.client:
                try:
                    async with self.client.get_queue_receiver(
                        queue_name=settings.queue_name,
                        session_id=NEXT_AVAILABLE_SESSION,
                        max_session_lock_renewal_duration=300,  # Renew lock for up to 5 minutes
                    ) as receiver:
                        # Only proceed if we have a valid receiver with a session
                        if receiver.session:
                            session_id = receiver.session.session_id
                            logger.info(f"Processing session: {session_id}")

                            # Process all available messages in this session
                            await self._process_session(receiver, max_message_count, max_wait_time)
                except Exception as session_error:
                    # This is expected when no sessions are available
                    logger.info(f"No sessions available: {str(session_error)}")
        except Exception as e:
            # This would be an unexpected error
            logger.error(f"Unexpected error in process_messages: {str(e)}", exc_info=True)

    async def _process_session(self, receiver: ServiceBusReceiver, max_message_count: int, max_wait_time: int):
        """Process all available messages in a session."""
        session_id = receiver.session.session_id

        # Keep processing messages until none are left in this session
        while True:
            # Try to receive a batch of messages with a timeout
            messages = await receiver.receive_messages(max_message_count=max_message_count, max_wait_time=max_wait_time)

            # If no messages received, we're done with this session for now
            if not messages:
                logger.info(f"No more messages in session {session_id}")
                break

            logger.info(f"Received {len(messages)} messages from session {session_id}")

            # Process each message in the batch
            for msg in messages:
                try:
                    # Parse the message
                    data = json.loads(str(msg))
                    whatsapp_msg = WhatsAppQueueMessage.model_validate(data)

                    logger.info(f"Processing message: {whatsapp_msg.sms_message_sid} from session {session_id}")

                    # Process the message
                    result = await self.process_func(whatsapp_msg)

                    logger.info(f"Message processed with status: {result.get('status', 'unknown')}, SID: {result.get('message_sid', 'N/A')}")

                    # Complete the message
                    await receiver.complete_message(msg)
                except Exception as e:
                    # Log the error and abandon the message
                    logger.error(f"Error processing message: {str(e)}")
                    await receiver.abandon_message(msg)
