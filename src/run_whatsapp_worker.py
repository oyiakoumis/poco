#!/usr/bin/env python
"""Script to run the WhatsApp message worker."""

import asyncio

from messaging.worker import run_worker
from utils.logging import logger

if __name__ == "__main__":
    logger.info("Starting WhatsApp message worker")
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("WhatsApp message worker stopped by user")
    except Exception as e:
        logger.error(f"Error running WhatsApp message worker: {str(e)}")
        raise
