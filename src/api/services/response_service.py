"""Response formatting utilities for WhatsApp messages."""

import asyncio
import base64
from typing import Any, Dict, List, Optional

from twilio.base.exceptions import TwilioRestException
from twilio.http.async_http_client import AsyncTwilioHttpClient
from twilio.rest import Client

from agents.assistant import Assistant
from api.services.media_service import MediaService
from api.utils.text import MessageType, build_notification_string, format_message
from settings import settings
from utils.logging import logger


class ResponseService:
    """Utility class for formatting and sending WhatsApp responses."""

    def __init__(self):
        """Initialize the response formatter with Twilio client and media service."""
        http_client = AsyncTwilioHttpClient()
        self.twilio_client = Client(settings.twilio_account_sid, settings.twilio_auth_token, http_client=http_client)
        self.media_service = MediaService()

    async def send_acknowledgment(self, to_number: str, user_message: str, new_conversation: bool = False) -> None:
        """Send an acknowledgment message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            new_conversation: Whether a new conversation was created
        """
        response_message = "`🚀 Got it! Just give me a second...`"

        # Build concise notification string if needed
        notification_str = build_notification_string({"new_conversation": new_conversation})

        if notification_str:
            response_message += f"\n\n`{notification_str}`"

        # Format the response with the user's message
        formatted_response = format_message(user_message, response_message)

        # Send the acknowledgment message
        await self._send_message(to_number, formatted_response)

    async def send_error(self, to_number: str, user_message: str, error_message: str) -> None:
        """Send an error message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            error_message: The error message to send
        """
        formatted_error = format_message(user_message, error_message, message_type=MessageType.ERROR)
        await self._send_message(to_number, formatted_error)

    async def send_processing(self, to_number: str, user_message: str, processing_message: str) -> None:
        """Send a processing status message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            processing_message: The processing message to send
        """
        formatted_message = format_message(user_message, processing_message, message_type=MessageType.PROCESSING)
        await self._send_message(to_number, formatted_message)

    async def send_response(
        self, to_number: str, user_message: str, response_content: str, total_tokens: int = 0, file_attachments: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Send a response message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            response_content: The response content to send
            total_tokens: The total tokens used in the conversation
            file_attachments: Optional list of file attachments to send
        """
        # Add long chat notification if tokens exceed the limit
        if total_tokens > Assistant.TOKEN_LIMIT:
            notification_str = build_notification_string({"long_chat": True})
            response_content += f"\n\n`{notification_str}`"

        formatted_response = format_message(user_message, response_content)

        # Send the message with optional file attachments
        await self._send_message(to_number, formatted_response, file_attachments)

    def _split_message(self, message: str, max_length: int = 1600, max_parts: int = 10) -> list:
        """Split a message into multiple parts if it exceeds the maximum length.

        Args:
            message: The message to split
            max_length: The maximum length of each part
            max_parts: The maximum number of parts to split into

        Returns:
            A list of message parts
        """
        if len(message) <= max_length:
            return [message]

        # Calculate the total number of parts needed (capped at max_parts)
        total_parts = min(max_parts, (len(message) + max_length - 1) // max_length)

        # Reserve space for part indicator (e.g., "[1/3] ")
        part_indicator_length = len(f" [{total_parts}/{total_parts}]")
        effective_max_length = max_length - part_indicator_length

        parts = []
        remaining = message
        part_count = 0

        while remaining and part_count < max_parts:
            part_count += 1

            # If this is the last allowed part or the remaining text fits in one part
            if part_count == max_parts or len(remaining) <= effective_max_length:
                # For the last part, if we're truncating, add an indicator
                if part_count == max_parts and len(remaining) > effective_max_length:
                    part = remaining[: effective_max_length - 30] + "... (message truncated)"
                else:
                    part = remaining[:effective_max_length]

                # Add part indicator
                parts.append(f"{part} ({part_count}/{total_parts})")
                break

            # Find a good breaking point (preferably at a paragraph or sentence)
            cut_point = min(effective_max_length, len(remaining))

            # Try to break at a paragraph
            paragraph_break = remaining[:cut_point].rfind("\n\n")
            if paragraph_break > effective_max_length * 0.5:  # Only use if it's not too short
                cut_point = paragraph_break + 2  # Include the newlines
            else:
                # Try to break at a sentence
                sentence_break = remaining[:cut_point].rfind(". ")
                if sentence_break > effective_max_length * 0.5:  # Only use if it's not too short
                    cut_point = sentence_break + 2  # Include the period and space
                else:
                    # Fall back to breaking at a space
                    space_break = remaining[:cut_point].rfind(" ")
                    if space_break > 0:
                        cut_point = space_break + 1  # Include the space

            part = remaining[:cut_point]
            # Add part number indicator
            parts.append(f"{part} ({part_count}/{total_parts})")
            remaining = remaining[cut_point:]

        return parts

    async def _wait_for_message_delivery(self, message_sid: str, timeout: int = 10, poll_interval: float = 0.5):
        """Wait for a message to be processed enough to send the next message."""
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                # Fetch the message status
                message = await self.twilio_client.messages(message_sid).fetch_async()
                status = message.status

                logger.debug(f"Message {message_sid} status: {status}")

                # Consider "sent" or "delivered" as processed enough to continue
                if status in ["sent", "delivered"]:
                    logger.info(f"Message {message_sid} processed with status: {status}")
                    return

                # Log but continue for failed states
                if status in ["failed", "undelivered"]:
                    logger.error(f"Message {message_sid} has failed status")
                    raise Exception(f"Message {message_sid} failed to deliver")

                # Wait before checking again
                await asyncio.sleep(poll_interval)

            except TwilioRestException as e:
                logger.error(f"Error checking message status for {message_sid}: {e.msg}")
                # Continue with next message even on error
                return

        logger.warning(f"Timed out waiting for message {message_sid} status, continuing with next message")
        return

    async def _send_message(self, to_number: str, message_body: str, file_attachments: Optional[List[Dict[str, Any]]] = None) -> None:
        """Send a message using the Twilio client, optionally with file attachments.

        Args:
            to_number: The phone number to send the message to
            message_body: The message body to send
            file_attachments: Optional list of file attachments to send
        """
        try:
            # Prepare media_url if file attachments are provided (only use the first attachment)
            media_urls = []
            if file_attachments and len(file_attachments) > 0:
                # Only process the first attachment
                attachment = file_attachments[0]
                content_type = attachment.get("content_type")
                content = attachment.get("content")
                filename = attachment.get("filename", "attachment")
                media_url = await self.media_service.prepare_outgoing_attachment(content=content, filename=filename, content_type=content_type)
                media_urls.append(media_url)
                logger.info(f"Sending message with first file attachment to {to_number}")
                if len(file_attachments) > 1:
                    logger.warning(f"Ignoring {len(file_attachments) - 1} additional attachment(s) as only the first one is used")

            # Check message length before sending
            if len(message_body) > settings.twilio_max_message_length:
                logger.warning(f"Message exceeds Twilio's 1600 character limit: {len(message_body)} chars")

                # Split the message into multiple parts
                message_parts = self._split_message(message_body, max_length=settings.twilio_max_message_length - 50, max_parts=10)
                logger.info(f"Splitting message into {len(message_parts)} parts")

                # Send each part (only attach files to the first part)
                for i, part in enumerate(message_parts):
                    # Only include media_urls with the first part
                    part_media_urls = media_urls if i == 0 else None
                    message = await self.twilio_client.messages.create_async(
                        body=part, from_=settings.twilio_phone_number, to=to_number, media_url=part_media_urls
                    )
                    logger.info(f"Sent part {i+1}/{len(message_parts)} to {to_number} (SID: {message.sid})")

                    # Wait for message delivery if it contains media to preserve order
                    if part_media_urls:
                        await self._wait_for_message_delivery(message.sid)
            else:
                # Send as a single message
                message = await self.twilio_client.messages.create_async(
                    body=message_body, from_=settings.twilio_phone_number, to=to_number, media_url=media_urls if media_urls else None
                )

                # If message contains media, wait for delivery confirmation to avoid race condition
                if media_urls:
                    await self._wait_for_message_delivery(message.sid)
        except TwilioRestException as e:
            logger.error(f"Error sending message to {to_number}: {e.msg}")
            raise
