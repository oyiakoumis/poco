"""Message buffer for Azure Service Bus messages."""

import asyncio
import time
from typing import Dict, Generic, List, Tuple, TypeVar

from azure.servicebus import ServiceBusMessage

from utils.logging import logger

T = TypeVar("T")


class MessageBuffer(Generic[T]):
    """Buffer for collecting messages for a specified time before processing."""

    def __init__(self, buffer_time: float = 2.0):
        """Initialize the buffer with a specified wait time."""
        self.buffer_time = buffer_time
        self.buffers: Dict[str, List[Tuple[ServiceBusMessage, T]]] = {}
        self.buffer_timers: Dict[str, float] = {}
        self.processing_locks: Dict[str, asyncio.Lock] = {}
        self.processing_flags: Dict[str, bool] = {}  # Track if a session is being processed

    async def add_message(self, session_id: str, msg: ServiceBusMessage, parsed_msg: T) -> bool:
        """Add a message to the buffer for the specified session."""
        # Get or create lock for this session
        if session_id not in self.processing_locks:
            self.processing_locks[session_id] = asyncio.Lock()

        async with self.processing_locks[session_id]:
            # Initialize buffer for this session if it doesn't exist
            if session_id not in self.buffers:
                self.buffers[session_id] = []

            # Check if we should start a new timer
            # Only start a timer if we're not currently processing and there's no timer
            is_first = False
            if not self.is_processing(session_id) and session_id not in self.buffer_timers:
                self.buffer_timers[session_id] = time.time()
                is_first = True

            # Add message to buffer
            self.buffers[session_id].append((msg, parsed_msg))
            logger.info(f"Added message to buffer for session {session_id}. Buffer size: {len(self.buffers[session_id])}")

            return is_first

    async def get_messages(self, session_id: str) -> List[Tuple[ServiceBusMessage, T]]:
        """Get and clear all messages for the specified session."""
        if session_id not in self.processing_locks:
            return []

        async with self.processing_locks[session_id]:
            if session_id not in self.buffers or not self.buffers[session_id]:
                return []

            # Mark this session as processing
            self.processing_flags[session_id] = True

            # Get messages and clear buffer
            messages = self.buffers[session_id]
            self.buffers[session_id] = []

            # Clear timer
            if session_id in self.buffer_timers:
                del self.buffer_timers[session_id]

            return messages

    def is_buffer_ready(self, session_id: str) -> bool:
        """Check if the buffer wait time has elapsed for the specified session."""
        if session_id not in self.buffer_timers:
            return False

        elapsed = time.time() - self.buffer_timers[session_id]
        return elapsed >= self.buffer_time

    def is_processing(self, session_id: str) -> bool:
        """Check if the session is currently being processed."""
        return self.processing_flags.get(session_id, False)

    async def set_processing_done(self, session_id: str):
        """Mark processing as done for the session."""
        if session_id not in self.processing_locks:
            return

        async with self.processing_locks[session_id]:
            self.processing_flags[session_id] = False

            # If there are new messages in the buffer, start a new timer
            if session_id in self.buffers and self.buffers[session_id]:
                self.buffer_timers[session_id] = time.time()
                return True

            return False
