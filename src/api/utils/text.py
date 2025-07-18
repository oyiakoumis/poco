"""Text utility functions."""

from enum import Enum, auto


def trim_message(message: str, max_length: int = 50) -> str:
    """Trim a message to a specified maximum length while preserving whole words."""
    if not message:
        return ""

    if len(message) <= max_length:
        return message

    # Find the last space before the max_length
    trimmed = message[:max_length]
    last_space = trimmed.rfind(" ")

    if last_space > 0:
        trimmed = trimmed[:last_space]

    return f"{trimmed} [...]"


class MessageType(Enum):
    """Enum for message types."""

    NORMAL = auto()
    ERROR = auto()
    PROCESSING = auto()


def format_message(user_message: str, response: str, message_type: MessageType = MessageType.NORMAL) -> str:
    """Format a message with the user's message and the response"""
    # Split at first line break and take only the first line for WhatsApp quote
    first_line = user_message.split("\n")[0] if user_message else ""
    trimmed_user_message = trim_message(first_line)

    if not trimmed_user_message:
        trimmed_user_message = "_empty message_"

    formatted_message = f"> {trimmed_user_message}\n"

    if message_type in (MessageType.PROCESSING, MessageType.ERROR):
        emoji = "❌" if message_type == MessageType.ERROR else "🔄"
        if "\n" in response:
            formatted_message += f"```\n{emoji} {response}\n```"
        else:
            formatted_message += f"`{emoji} {response}`"
    else:
        formatted_message += f"{response}"

    return formatted_message


def build_notification_string(flags: dict) -> str:
    """Build a concise notification string based on flag conditions.

    Args:
        flags: Dictionary of notification flags
            - new_conversation: True if a new conversation was created
            - long_chat: True if the chat exceeds the token limit

    Returns:
        A concise notification string with icons, or empty string if no notifications
    """
    parts = []
    if flags.get("new_conversation"):
        parts.append("🆕 New chat")
    if flags.get("long_chat"):
        parts.append("🧵 Long chat")

    return " | ".join(parts) if parts else ""


class Command(str, Enum):
    """Enum for WhatsApp commands."""

    NEW_CONVERSATION = "/new"


def is_command(message: str, command: str) -> bool:
    """Check if the message starts with a specific command."""
    return message.strip().startswith(command)


def extract_message_after_command(message: str, command: str) -> str:
    """Extract the message content after a command."""
    if not message.strip().startswith(command):
        return message

    return message[message.find(command) + len(command) :].strip()
