"""Consumer for processing messages from Azure Service Bus."""

import asyncio
import json
from typing import Awaitable, Callable, Dict, List, Optional

from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver
from azure.servicebus.exceptions import OperationTimeoutError
from twilio.rest import Client

from config import settings
from config import settings as api_settings
from messaging.buffer import MessageBuffer
from messaging.models import WhatsAppQueueMessage
from utils.logging import logger


class WhatsAppMessageConsumer:
    """Consumer for processing WhatsApp messages from Azure Service Bus."""

    def __init__(self, process_func: Callable[[WhatsAppQueueMessage], Awaitable[dict]], buffer_time: float = 1.0):
        """Initialize the consumer with settings and processing function."""
        self.process_func = process_func
        self.twilio_client = Client(api_settings.twilio_account_sid, api_settings.twilio_auth_token)
        self.client = ServiceBusClient.from_connection_string(conn_str=settings.connection_string)
        self.message_buffer = MessageBuffer[WhatsAppQueueMessage](buffer_time=buffer_time)
        self.processing_tasks: Dict[str, asyncio.Task] = {}

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
                except OperationTimeoutError:
                    # This is expected when no sessions are available
                    pass
        except Exception as e:
            # This would be an unexpected error
            logger.error(f"Unexpected error in process_messages: {str(e)}", exc_info=True)

    async def _process_session(self, receiver: ServiceBusReceiver, max_message_count: int, max_wait_time: int):
        """Process all available messages in a session."""
        session_id = receiver.session.session_id

        # Keep track of whether we need to process the buffer
        buffer_needs_processing = False

        # Keep processing messages until none are left in this session
        while True:
            # Try to receive a batch of messages with a timeout
            messages = await receiver.receive_messages(max_message_count=max_message_count, max_wait_time=max_wait_time)

            # If no messages received, check if we need to process the buffer
            if not messages:
                logger.info(f"No more messages in session {session_id}")

                # Check if we have any buffered messages to process
                if self.message_buffer.is_buffer_ready(session_id) or buffer_needs_processing:
                    await self._process_buffer(receiver, session_id)

                    # Check if new messages arrived during processing
                    buffer_needs_processing = await self.message_buffer.set_processing_done(session_id)

                    # If no new messages, we're done
                    if not buffer_needs_processing:
                        break
                else:
                    # No messages and no buffer to process, we're done
                    break
            else:
                logger.info(f"Received {len(messages)} messages from session {session_id}")

                # Add messages to the buffer
                for msg in messages:
                    try:
                        # Parse the message
                        data = json.loads(str(msg))
                        whatsapp_msg = WhatsAppQueueMessage.model_validate(data)

                        logger.info(f"Buffering message: {whatsapp_msg.sms_message_sid} from session {session_id}")

                        # Add to buffer
                        is_first = await self.message_buffer.add_message(session_id, msg, whatsapp_msg)

                        # If this is the first message, mark that we need to process the buffer
                        if is_first:
                            buffer_needs_processing = True
                    except Exception as e:
                        # Log the error and abandon the message
                        logger.error(f"Error parsing message: {str(e)}")
                        await receiver.abandon_message(msg)

                # Process the buffer if it's ready
                if self.message_buffer.is_buffer_ready(session_id):
                    await self._process_buffer(receiver, session_id)
                    buffer_needs_processing = await self.message_buffer.set_processing_done(session_id)

    async def _process_buffer(self, receiver: ServiceBusReceiver, session_id: str):
        """Process messages in the buffer, starting with the earliest one."""
        # Get all messages from the buffer
        buffered_messages = await self.message_buffer.get_messages(session_id)

        if not buffered_messages:
            # No messages to process
            return

        logger.info(f"Processing buffer for session {session_id} with {len(buffered_messages)} messages")

        # Sort messages by created_at timestamp
        sorted_messages = sorted(buffered_messages, key=lambda x: x[1].created_at)

        # Process the earliest message first
        if sorted_messages:
            earliest_msg, earliest_whatsapp_msg = sorted_messages[0]
            try:
                logger.info(f"Processing earliest message: {earliest_whatsapp_msg.sms_message_sid} from session {session_id}")

                # Process the message
                result = await self.process_func(earliest_whatsapp_msg)

                logger.info(f"Earliest message processed with status: {result.get('status', 'unknown')}, SID: {result.get('message_sid', 'N/A')}")

                # Complete the message
                await receiver.complete_message(earliest_msg)
            except Exception as e:
                # Log the error and abandon the message
                logger.error(f"Error processing earliest message: {str(e)}")
                await receiver.abandon_message(earliest_msg)

        # Just complete the rest of the messages without processing
        for msg, whatsapp_msg in sorted_messages[1:]:
            try:
                logger.info(f"Completing remaining message without processing: {whatsapp_msg.sms_message_sid} from session {session_id}")

                # Complete the message without processing
                await receiver.complete_message(msg)
            except Exception as e:
                # Log the error and abandon the message
                logger.error(f"Error completing message: {str(e)}")
                await receiver.abandon_message(msg)
