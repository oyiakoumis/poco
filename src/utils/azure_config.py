"""Azure Configuration Provider.

This module provides a unified interface for accessing configuration values
from Azure App Configuration and secrets from Azure Key Vault.
"""

import logging
import os
from typing import Any, Dict, Optional, Set

from azure.appconfiguration import AzureAppConfigurationClient
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from utils.singleton import Singleton

logger = logging.getLogger(__name__)


class AzureConfigProvider(metaclass=Singleton):

    def __init__(
        self,
        app_config_endpoint: Optional[str] = None,
        key_vault_url: Optional[str] = None,
        app_config_keys: Optional[Set[str]] = None,
        key_vault_secrets: Optional[Set[str]] = None,
    ):
        """Initialize the Azure Configuration Provider."""
        self.is_local = os.environ.get("IS_LOCAL", "false").lower() in ("true", "1", "t", "yes", "y")
        self.app_config_endpoint = app_config_endpoint or os.environ.get("APP_CONFIG_ENDPOINT")
        self.key_vault_url = key_vault_url or os.environ.get("KEY_VAULT_URL")

        # Initialize credential based on environment
        self.credential = DefaultAzureCredential(additionally_allowed_tenants=["*"])

        # Initialize clients if endpoints are provided
        self.app_config_client = self._initialize_app_config_client() if self.app_config_endpoint else None
        self.key_vault_client = self._initialize_key_vault_client() if self.key_vault_url else None

        # Initialize configuration stores
        self.config_values: Dict[str, Any] = {}
        self.secrets: Dict[str, Any] = {}

        # Load configuration values
        if app_config_keys and self.app_config_client:
            self._load_app_config_values(app_config_keys)

        # Load secrets
        if key_vault_secrets and self.key_vault_client:
            self._load_key_vault_secrets(key_vault_secrets)

    def _initialize_app_config_client(self):
        """Initialize the Azure App Configuration client."""
        return AzureAppConfigurationClient(
            endpoint=self.app_config_endpoint,
            credential=self.credential,
        )

    def _initialize_key_vault_client(self):
        """Initialize the Azure Key Vault client."""
        return SecretClient(
            vault_url=self.key_vault_url,
            credential=self.credential,
        )

    def _load_app_config_values(self, keys: Set[str]):
        """Load configuration values from Azure App Configuration."""
        for key in keys:
            try:
                setting = self.app_config_client.get_configuration_setting(key=key)
                self.config_values[key] = setting.value
                logger.debug(f"Loaded configuration value for key: {key}")
            except ResourceNotFoundError:
                # Fallback to environment variable
                env_value = os.environ.get(key)
                if env_value is not None:
                    self.config_values[key] = env_value
                    logger.debug(f"Using environment variable for key: {key}")
                else:
                    logger.warning(f"Configuration key not found: {key}")

    def _load_key_vault_secrets(self, secret_names: Set[str]):
        """Load secrets from Azure Key Vault."""
        for name in secret_names:
            try:
                secret = self.key_vault_client.get_secret(name=name)
                self.secrets[name] = secret.value
                logger.debug(f"Loaded secret: {name}")
            except ResourceNotFoundError:
                # Fallback to environment variable
                env_value = os.environ.get(name)
                if env_value is not None:
                    self.secrets[name] = env_value
                    logger.debug(f"Using environment variable for secret: {name}")
                else:
                    logger.warning(f"Secret not found: {name}")

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.config_values.get(key, os.environ.get(key, default))

    def get_secret(self, key: str, default: Any = None) -> Any:
        """Get a secret."""
        return self.secrets.get(key, os.environ.get(key, default))
