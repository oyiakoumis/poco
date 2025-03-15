"""Response formatting utilities for WhatsApp messages."""

from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse

from settings import settings
from utils.logging import logger
from api.utils.text import MessageType, build_notification_string, format_message


class ResponseFormatter:
    """Utility class for formatting and sending WhatsApp responses."""

    def __init__(self):
        """Initialize the response formatter with Twilio client."""
        self.twilio_client = Client(settings.twilio_account_sid, settings.twilio_auth_token)

    def send_acknowledgment(self, to_number: str, user_message: str, new_conversation: bool = False, unsupported_media: bool = False) -> None:
        """Send an acknowledgment message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            new_conversation: Whether a new conversation was created
            unsupported_media: Whether unsupported media was received
        """
        response_message = "`🚀 Got it! Just give me just a second...`"

        # Build concise notification string if needed
        notification_str = build_notification_string({"new_conversation": new_conversation, "unsupported_media": unsupported_media})

        if notification_str:
            response_message += f"\n\n`{notification_str}`"

        # Format the response with the user's message
        formatted_response = format_message(user_message, response_message)

        # Send the acknowledgment message
        self._send_message(to_number, formatted_response)

    def send_error(self, to_number: str, user_message: str, error_message: str) -> None:
        """Send an error message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            error_message: The error message to send
        """
        formatted_error = format_message(user_message, error_message, message_type=MessageType.ERROR)
        self._send_message(to_number, formatted_error)
        
    def send_processing(self, to_number: str, user_message: str, processing_message: str) -> None:
        """Send a processing status message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            processing_message: The processing message to send
        """
        formatted_message = format_message(user_message, processing_message, message_type=MessageType.PROCESSING)
        self._send_message(to_number, formatted_message)

    def send_response(self, to_number: str, user_message: str, response_content: str) -> None:
        """Send a response message to the user.

        Args:
            to_number: The phone number to send the message to
            user_message: The original user message
            response_content: The response content to send
        """
        formatted_response = format_message(user_message, response_content)
        self._send_message(to_number, formatted_response)

    def _send_message(self, to_number: str, message_body: str) -> None:
        """Send a message using the Twilio client.

        Args:
            to_number: The phone number to send the message to
            message_body: The message body to send
        """
        try:
            self.twilio_client.messages.create(body=message_body, from_=settings.twilio_phone_number, to=to_number)
        except Exception as e:
            logger.error(f"Error sending message to {to_number}: {str(e)}")
