"""Azure Blob Storage distributed lock manager."""

from typing import List, Optional, Union
from uuid import UUID

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobClient,
    BlobLeaseClient,
    BlobServiceClient,
)

from settings import settings
from utils.logging import logger
from utils.singleton import Singleton


class AzureBlobLockManager(metaclass=Singleton):
    """Distributed lock manager using Azure Blob Storage leases."""

    def __init__(self, lock_timeout_seconds: int = 60):
        """Initialize the Azure Blob Storage lock manager with connection parameters from settings.

        Args:
            lock_timeout_seconds: Duration of the lease in seconds. Must be between 15 and 60 seconds,
                                 or -1 for infinite lease.
        """
        self.account_name = settings.azure_storage_account
        self.container_name = f"{settings.azure_storage_container}-locks"  # Dedicated container for locks
        self.account_url = f"https://{self.account_name}.blob.core.windows.net"

        # Azure Blob lease duration must be between 15 and 60 seconds, or -1 for infinite
        self.lock_timeout_seconds = max(15, min(60, lock_timeout_seconds))

        # Create the credential
        self.credential = DefaultAzureCredential()

        # Create the blob service client
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url,
            credential=self.credential
        )

        # Ensure the container exists
        self._ensure_container_exists()

    def _ensure_container_exists(self):
        """Ensure the container for locks exists."""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created lock container: {self.container_name}")
        except Exception as e:
            logger.error(f"Error ensuring lock container exists: {str(e)}")
            raise ValueError(f"Could not create lock container: {str(e)}")

    def _get_blob_client(self, conversation_id: UUID) -> BlobClient:
        """Get a blob client for the given conversation ID.

        Args:
            conversation_id: The UUID of the conversation to get a blob client for.

        Returns:
            A BlobClient for the conversation's lock blob.
        """
        resource_name = f"conversation_lock_{str(conversation_id)}"
        return self.blob_service_client.get_blob_client(container=self.container_name, blob=resource_name)

    def _ensure_blob_exists(self, blob_client: BlobClient) -> None:
        """Ensure the blob exists by creating it if it doesn't.

        Args:
            blob_client: The BlobClient for the blob to ensure exists.
        """
        try:
            blob_client.upload_blob(b"", overwrite=False)
        except ResourceExistsError:
            # Blob already exists, which is fine
            pass
        except Exception as e:
            logger.error(f"Error creating blob: {str(e)}")
            raise

    def acquire_lock(self, conversation_id: UUID, lease_duration: Optional[int] = None) -> Optional[dict]:
        """Acquire a distributed lock for the given conversation ID.

        Args:
            conversation_id: The UUID of the conversation to acquire a lock for.
            lease_duration: Optional duration of the lease in seconds. If not provided,
                           the default lock_timeout_seconds will be used.

        Returns:
            A dictionary containing the lock information if successful, None otherwise.
        """
        blob_client = self._get_blob_client(conversation_id)
        lease_duration = lease_duration or self.lock_timeout_seconds
        resource_name = f"conversation_lock_{str(conversation_id)}"

        try:
            # Create the blob if it doesn't exist
            self._ensure_blob_exists(blob_client)

            # Create a lease client and acquire a lease
            lease_client = BlobLeaseClient(client=blob_client)
            lease_client.acquire(lease_duration=lease_duration)

            if lease_client.id:
                logger.info(f"Acquired lock for conversation {conversation_id}")
                # Return a dict similar to Redlock for compatibility
                return {
                    "resource": resource_name,
                    "lease_id": lease_client.id,
                    "validity": lease_duration,
                }
            else:
                logger.info(f"Failed to acquire lock for conversation {conversation_id}")
                return None
        except Exception as e:
            if "LeaseAlreadyPresent" in str(e):
                logger.info(f"Lock already exists for conversation {conversation_id}")
                return None
            logger.error(f"Error acquiring lock for conversation {conversation_id}: {str(e)}")
            return None

    def renew_lock(self, lock: dict) -> bool:
        """Renew a previously acquired lock.

        Args:
            lock: The lock dictionary returned by acquire_lock.

        Returns:
            True if the lock was successfully renewed, False otherwise.
        """
        if not lock or "resource" not in lock or "lease_id" not in lock:
            logger.error("Invalid lock object provided for renewal")
            return False

        resource_name = lock["resource"]
        lease_id = lock["lease_id"]

        # Extract conversation_id from resource_name
        try:
            conversation_id_str = resource_name.split("_")[-1]
            conversation_id = UUID(conversation_id_str)
            blob_client = self._get_blob_client(conversation_id)
        except Exception as e:
            logger.error(f"Error extracting conversation ID from resource name: {str(e)}")
            return False

        try:
            # Create a lease client with the existing lease ID
            lease_client = BlobLeaseClient(client=blob_client, lease_id=lease_id)

            # Renew the lease
            lease_client.renew()
            logger.info(f"Renewed lock for resource {resource_name}")
            return True
        except Exception as e:
            logger.error(f"Error renewing lock: {str(e)}")
            return False

    def release_lock(self, lock: dict) -> bool:
        """Release a previously acquired lock.

        Args:
            lock: The lock dictionary returned by acquire_lock.

        Returns:
            True if the lock was successfully released, False otherwise.
        """
        if not lock or "resource" not in lock or "lease_id" not in lock:
            logger.error("Invalid lock object provided for release")
            return False

        resource_name = lock["resource"]
        lease_id = lock["lease_id"]

        # Extract conversation_id from resource_name
        try:
            conversation_id_str = resource_name.split("_")[-1]
            conversation_id = UUID(conversation_id_str)
            blob_client = self._get_blob_client(conversation_id)
        except Exception as e:
            logger.error(f"Error extracting conversation ID from resource name: {str(e)}")
            return False

        try:
            # Create a lease client with the existing lease ID
            lease_client = BlobLeaseClient(client=blob_client, lease_id=lease_id)

            # Release the lease
            lease_client.release()
            logger.info(f"Released lock for resource {resource_name}")
            return True
        except Exception as e:
            logger.error(f"Error releasing lock: {str(e)}")
            return False

    def break_lock(self, conversation_id: UUID, break_period: int = 0) -> bool:
        """Break a lock for the given conversation ID.

        Args:
            conversation_id: The UUID of the conversation to break the lock for.
            break_period: The time to wait, in seconds, before the lock is broken.
                         If 0, the lock is broken immediately.

        Returns:
            True if the lock was successfully broken, False otherwise.
        """
        blob_client = self._get_blob_client(conversation_id)

        try:
            # Create a lease client
            lease_client = BlobLeaseClient(client=blob_client)

            # Break the lease
            broken_lease = lease_client.break_lease(lease_break_period=break_period)
            logger.info(f"Broke lock for conversation {conversation_id}, time remaining: {broken_lease}")
            return True
        except ResourceNotFoundError:
            # Blob doesn't exist, so there's no lock to break
            logger.info(f"No lock exists for conversation {conversation_id}")
            return True
        except Exception as e:
            logger.error(f"Error breaking lock for conversation {conversation_id}: {str(e)}")
            return False

    def is_locked(self, conversation_id: UUID) -> bool:
        """Check if a conversation is currently locked.

        Args:
            conversation_id: The UUID of the conversation to check.

        Returns:
            True if the conversation is locked, False otherwise.
        """
        blob_client = self._get_blob_client(conversation_id)

        try:
            # Check if the blob exists
            try:
                properties = blob_client.get_blob_properties()
            except ResourceNotFoundError:
                # Blob doesn't exist, so it's not locked
                return False

            # If the blob has an active lease, it's locked
            if properties.lease.state == "leased":
                return True

            # Try to acquire a lease with minimum duration
            # If we can acquire it, it's not locked
            lease_client = BlobLeaseClient(client=blob_client)
            lease_client.acquire(lease_duration=15)
            if lease_client.id:
                # Release it immediately
                lease_client.release()
                return False
            return True
        except Exception as e:
            # If we get an error trying to acquire the lease, it's likely because
            # the blob is already leased (locked)
            if "LeaseAlreadyPresent" in str(e):
                return True

            logger.error(f"Error checking lock status for conversation {conversation_id}: {str(e)}")
            # In case of error, assume it's not locked to avoid blocking messages
            return False

    def close(self) -> None:
        """Close the blob service client."""
        try:
            self.blob_service_client.close()
            logger.info("Azure Blob Storage client closed")
        except Exception as e:
            logger.error(f"Error closing Azure Blob Storage client: {str(e)}")
