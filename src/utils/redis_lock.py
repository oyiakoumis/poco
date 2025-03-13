"""Redis distributed lock manager using redlock-py."""

import json
from typing import List, Optional
from uuid import UUID

import redis
from redlock import Redlock

from config import settings
from utils.logging import logger
from utils.singleton import Singleton


class RedLockManager(metaclass=Singleton):
    """Distributed lock manager using Redis and redlock-py."""

    def __init__(self):
        """Initialize the Redis lock manager with connection strings from settings."""
        self.redis_connections: List[redis.Redis] = []

        for conn_str in settings.redis_connection_strings:
            try:
                # Create Redis connection
                r = redis.Redis.from_url(conn_str, port=settings.redis_port, ssl=True, decode_responses=True)
                self.redis_connections.append(r)
                logger.info(f"Successfully connected to Redis: {conn_str}")
            except Exception as e:
                logger.error(f"Error creating Redis connection: {str(e)}")

        if not self.redis_connections:
            logger.error("No valid Redis connections could be established")
            raise ValueError("No valid Redis connections could be established")

        if len(self.redis_connections) < 3:
            logger.error(f"Redlock requires at least 3 Redis instances, but only {len(self.redis_connections)} were connected")
            raise ValueError(f"Redlock requires at least 3 Redis instances, but only {len(self.redis_connections)} were connected")

        # Initialize Redlock with Redis connections
        self.dlm = Redlock([{"connection": conn} for conn in self.redis_connections])
        self.lock_timeout_ms = settings.redis_lock_timeout_ms

    def acquire_lock(self, conversation_id: UUID) -> Optional[dict]:
        """Acquire a distributed lock for the given conversation ID.

        Args:
            conversation_id: The conversation ID to lock

        Returns:
            A lock object if successful, None otherwise
        """
        resource_name = f"conversation_lock:{str(conversation_id)}"
        try:
            lock = self.dlm.lock(resource_name, self.lock_timeout_ms)
            if lock:
                logger.info(f"Acquired lock for conversation {conversation_id}")
                return lock
            else:
                logger.info(f"Failed to acquire lock for conversation {conversation_id}")
                return None
        except Exception as e:
            logger.error(f"Error acquiring lock for conversation {conversation_id}: {str(e)}")
            return None

    def release_lock(self, lock: dict) -> bool:
        """Release a previously acquired lock.

        Args:
            lock: The lock object returned by acquire_lock

        Returns:
            True if the lock was released successfully, False otherwise
        """
        try:
            self.dlm.unlock(lock)
            logger.info(f"Released lock for resource {lock.get('resource', 'unknown')}")
            return True
        except Exception as e:
            logger.error(f"Error releasing lock: {str(e)}")
            return False

    def is_locked(self, conversation_id: UUID) -> bool:
        """Check if a conversation is currently locked.

        Args:
            conversation_id: The conversation ID to check

        Returns:
            True if the conversation is locked, False otherwise
        """
        resource_name = f"conversation_lock:{str(conversation_id)}"

        # Try to acquire with 0 timeout - if we can't, it's locked
        try:
            lock = self.dlm.lock(resource_name, 0)
            if lock:
                # We got the lock, so it wasn't locked - release it immediately
                self.dlm.unlock(lock)
                return False
            return True
        except Exception as e:
            logger.error(f"Error checking lock status for conversation {conversation_id}: {str(e)}")
            # In case of error, assume it's not locked to avoid blocking messages
            return False

    def close(self) -> None:
        """Close all Redis connections."""
        for conn in self.redis_connections:
            try:
                conn.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {str(e)}")
