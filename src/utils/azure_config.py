"""Azure Configuration Provider.

This module provides a unified interface for accessing configuration values
from Azure App Configuration and secrets from Azure Key Vault.
"""

import asyncio
import os
from typing import Any, Dict, Set

from azure.appconfiguration.aio import AzureAppConfigurationClient
from azure.core.exceptions import ResourceNotFoundError
from azure.keyvault.secrets.aio import SecretClient

from utils.azure_auth import get_azure_credential
from utils.logging import logger
from utils.singleton import Singleton


class AzureConfigProvider(metaclass=Singleton):

    def __init__(self, app_config_keys: Set[str], key_vault_secrets: Set[str]):
        """Initialize the Azure Configuration Provider."""
        self.is_local = os.environ.get("IS_LOCAL", "false").lower() in ("true", "1", "t", "yes", "y")
        self.app_config_base_url = os.environ["APP_CONFIG_URI"]
        self.key_vault_url = os.environ["KEY_VAULT_URI"]

        # Initialize credential based on environment
        self.credential = get_azure_credential()

        # Initialize configuration stores
        self.config_values: Dict[str, Any] = {}
        self.secrets: Dict[str, Any] = {}

        # Run the async initialization in a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._async_init(app_config_keys, key_vault_secrets))
        finally:
            loop.close()

    async def _async_init(self, app_config_keys: Set[str], key_vault_secrets: Set[str]):
        """Asynchronously initialize clients and load configuration values and secrets."""
        # Initialize clients
        self.app_config_client = AzureAppConfigurationClient(
            base_url=self.app_config_base_url,
            credential=self.credential,
        )

        self.key_vault_client = SecretClient(
            vault_url=self.key_vault_url,
            credential=self.credential,
        )

        # Load all configuration values and secrets concurrently
        await self._load_all_configs_and_secrets(app_config_keys, key_vault_secrets)

    async def _load_all_configs_and_secrets(self, app_config_keys: Set[str], key_vault_secrets: Set[str]):
        """Load all configuration values and secrets concurrently."""
        # Create tasks for all keys and secrets using list comprehensions
        config_tasks = [self._load_value(key, is_secret=False) for key in app_config_keys] if app_config_keys else []
        secret_tasks = [self._load_value(name, is_secret=True) for name in key_vault_secrets] if key_vault_secrets else []

        # Run all tasks concurrently
        if config_tasks or secret_tasks:
            await asyncio.gather(*(config_tasks + secret_tasks))

    async def _load_value(self, key: str, is_secret: bool):
        """Generic method to load a value from either App Configuration or Key Vault."""
        store_dict = self.secrets if is_secret else self.config_values
        client = self.key_vault_client if is_secret else self.app_config_client
        source_name = "secret" if is_secret else "configuration value"

        # Define the appropriate fetch operation based on the type
        fetch_op = client.get_secret if is_secret else client.get_configuration_setting
        fetch_args = {"name": key} if is_secret else {"key": key}

        try:
            result = await fetch_op(**fetch_args)
            store_dict[key] = result.value
            logger.debug(f"Loaded {source_name}: {key}")
        except ResourceNotFoundError:
            # Fallback to environment variable
            self._load_from_env(key, store_dict, source_name)
        except Exception as e:
            # Log error and fallback to environment variable
            logger.error(f"Error loading {source_name} {key}: {str(e)}")
            self._load_from_env(key, store_dict, source_name)

    def _load_from_env(self, key: str, store_dict: Dict[str, Any], source_name: str):
        """Load a value from environment variables as fallback."""
        env_value = os.environ.get(key)
        if env_value is not None:
            store_dict[key] = env_value
            logger.debug(f"Using environment variable for {source_name}: {key}")
        else:
            logger.warning(f"{source_name.capitalize()} not found: {key}")

    def _get_value(self, key: str, store_dict: Dict[str, Any], value_type: str, default: Any = None) -> Any:
        value = store_dict.get(key, os.environ.get(key, default))
        if value is None:
            raise ValueError(f"{value_type.capitalize()} not found: {key}")
        return value

    def get_config(self, key: str, default: Any = None) -> Any:
        return self._get_value(key, self.config_values, "configuration key", default)

    def get_secret(self, key: str, default: Any = None) -> Any:
        return self._get_value(key, self.secrets, "secret", default)
