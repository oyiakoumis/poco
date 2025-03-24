"""Azure Authentication utilities.

This module provides functions for Azure authentication based on the environment.
"""

import os

from azure import identity
from azure.identity import aio as identity_async

from utils.logging import logger


def get_azure_credential(do_async: bool = False):
    """Get the appropriate Azure credential based on the environment."""
    # Check if running locally
    is_local = os.environ.get("IS_LOCAL", "false").lower() in ("true", "1", "t", "yes", "y")
    module = identity_async if do_async else identity

    if is_local:
        # Use Service Principal authentication for local development
        tenant_id = os.environ["AZURE_TENANT_ID"]
        client_id = os.environ["AZURE_CLIENT_ID"]
        client_secret = os.environ["AZURE_CLIENT_SECRET"]

        logger.info("Using Service Principal authentication for local development")
        return module.ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    else:
        # Use User Managed Identity for non-local environments
        client_id = os.environ.get("AZURE_CLIENT_ID")
        if client_id:
            logger.info(f"Using User Managed Identity authentication with client ID: {client_id}")
            return module.ManagedIdentityCredential(client_id=client_id)
        else:
            logger.info("Using User Managed Identity authentication without client ID")
            return module.ManagedIdentityCredential()
