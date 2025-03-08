"""Text utility functions."""


def trim_message(message: str, max_length: int = 50) -> str:
    """
    Trim a message to a specified maximum length while preserving whole words.
    
    Args:
        message: The message to trim
        max_length: Maximum length of the trimmed message
        
    Returns:
        The trimmed message
    """
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
    """
    Format a message with the user's message and the response.
    
    Args:
        user_message: The user's message
        response: The response message
        is_error: Whether this is an error message
        
    Returns:
        The formatted message
    """
    trimmed_user_message = trim_message(user_message)
    
    if is_error:
        return f"In response to: \"{trimmed_user_message}\"\nError: {response}"
    else:
        return f"In response to: \"{trimmed_user_message}\"\n{response}"
