"""Response formatting utilities for WhatsApp messages."""

import base64
from typing import Any, Dict, Optional

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
        self, to_number: str, user_message: str, response_content: str, total_tokens: int = 0, file_attachment: Optional[Dict[str, Any]] = None
    ) -> None:
        """Send a response message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            response_content: The response content to send
            total_tokens: The total tokens used in the conversation
            file_attachment: Optional file attachment to send
        """
        # Add long chat notification if tokens exceed the limit
        if total_tokens > Assistant.TOKEN_LIMIT:
            notification_str = build_notification_string({"long_chat": True})
            response_content += f"\n\n`{notification_str}`"

        formatted_response = format_message(user_message, response_content)

        # Send the message with optional file attachment
        await self._send_message(to_number, formatted_response, file_attachment)

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

        parts = []
        remaining = message
        part_count = 0

        while remaining and part_count < max_parts:
            part_count += 1

            # If this is the last allowed part or the remaining text fits in one part
            if part_count == max_parts or len(remaining) <= max_length:
                # For the last part, if we're truncating, add an indicator
                if part_count == max_parts and len(remaining) > max_length:
                    part = remaining[: max_length - 30] + "... (message truncated)"
                else:
                    part = remaining[:max_length]
                parts.append(f"{part} ({part_count}/{total_parts})")
                break

            # Find a good breaking point (preferably at a paragraph or sentence)
            cut_point = min(max_length, len(remaining))

            # Try to break at a paragraph
            paragraph_break = remaining[:cut_point].rfind("\n\n")
            if paragraph_break > max_length * 0.5:  # Only use if it's not too short
                cut_point = paragraph_break + 2  # Include the newlines
            else:
                # Try to break at a sentence
                sentence_break = remaining[:cut_point].rfind(". ")
                if sentence_break > max_length * 0.5:  # Only use if it's not too short
                    cut_point = sentence_break + 2  # Include the period and space
                else:
                    # Fall back to breaking at a space
                    space_break = remaining[:cut_point].rfind(" ")
                    if space_break > 0:
                        cut_point = space_break + 1  # Include the space

            part = remaining[:cut_point]
            # Add part number indicator with correct total
            parts.append(f"{part} ({part_count}/{total_parts})")
            remaining = remaining[cut_point:]

        return parts

    async def _send_message(self, to_number: str, message_body: str, file_attachment: Optional[Dict[str, Any]] = None) -> None:
        """Send a message using the Twilio client, optionally with a file attachment.

        Args:
            to_number: The phone number to send the message to
            message_body: The message body to send
            file_attachment: Optional file attachment to send
        """
        try:
            # Prepare media_url if file attachment is provided
            media_url = None
            if file_attachment:
                content_type = file_attachment.get("content_type")
                content = file_attachment.get("content")
                filename = file_attachment.get("filename", "attachment")
                media_url = await self.media_service.prepare_outgoing_attachment(content=content, filename=filename, content_type=content_type)
                logger.info(f"Sending message with file attachment '{filename}' to {to_number}")

            # Check message length before sending
            if len(message_body) > settings.twilio_max_message_length:
                logger.warning(f"Message exceeds Twilio's 1600 character limit: {len(message_body)} chars")

                # Split the message into multiple parts (max 10)
                message_parts = self._split_message(message_body, max_length=settings.twilio_max_message_length, max_parts=10)
                logger.info(f"Splitting message into {len(message_parts)} parts")

                # Send each part (only attach file to the first part)
                for i, part in enumerate(message_parts):
                    # Only include media_url with the first part
                    part_media_url = media_url if i == 0 else None
                    await self.twilio_client.messages.create_async(body=part, from_=settings.twilio_phone_number, to=to_number, media_url=[part_media_url])
                    logger.info(f"Sent part {i+1}/{len(message_parts)} to {to_number}")
            else:
                # Send as a single message
                await self.twilio_client.messages.create_async(body=message_body, from_=settings.twilio_phone_number, to=to_number, media_url=[media_url])
        except TwilioRestException as e:
            logger.error(f"Error sending message to {to_number}: {e.msg}")
            raise
