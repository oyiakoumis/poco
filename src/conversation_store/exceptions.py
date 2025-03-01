"""Exceptions for conversation store operations."""


class ConversationStoreError(Exception):
    """Base exception for conversation store errors."""

    pass


class ConversationNotFoundError(ConversationStoreError):
    """Raised when a conversation is not found."""

    pass


class MessageNotFoundError(ConversationStoreError):
    """Raised when a message is not found."""

    pass


class InvalidConversationError(ConversationStoreError):
    """Raised when conversation data is invalid."""

    pass


class InvalidMessageError(ConversationStoreError):
    """Raised when message data is invalid."""

    pass
