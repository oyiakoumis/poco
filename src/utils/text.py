"""Text utility functions."""


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

    return f"{trimmed}..."


def format_message(user_message: str, response: str, is_error: bool = False) -> str:
    """Format a message with the user's message and the response"""
    trimmed_user_message = trim_message(user_message)

    if is_error:
        # Error message with empathetic tone and helpful emojis
        error_message = f"⚠️ We encountered an issue with your request:\n"
        error_message += f'"{trimmed_user_message}"\n\n'
        error_message += f"🔧 {response}"
        return error_message
    else:
        # Regular message with friendly emojis
        regular_message = f"✨ In response to your message:\n"
        regular_message += f'"{trimmed_user_message}"\n\n'
        regular_message += f"📝 {response}"
        return regular_message
