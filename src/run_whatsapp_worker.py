#!/usr/bin/env python
"""Script to run the WhatsApp message worker."""

import asyncio
import sys

from messaging.consumer import WhatsAppMessageConsumer
from messaging.worker import process_whatsapp_message
from utils.logging import logger


async def run_worker():
    """Run the worker process to consume messages from the queue."""
    consumer = WhatsAppMessageConsumer(process_whatsapp_message)
    try:
        while True:
            await consumer.process_messages()
            await asyncio.sleep(0.1)  # Prevent CPU spinning
    except KeyboardInterrupt:
        logger.info("Worker process stopped")
    except Exception as e:
        logger.error(f"Worker process error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    logger.info("Starting WhatsApp message worker")
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("WhatsApp message worker stopped by user")
    except Exception as e:
        logger.error(f"Error running WhatsApp message worker: {str(e)}")
        raise
