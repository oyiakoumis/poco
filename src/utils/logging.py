"""Logging utilities for the application."""

import logging
import os
import sys

# Create and configure application logger
logger = logging.getLogger("poco")

logging_level = getattr(logging, os.environ.get("LOGGING_LEVEL", "DEBUG").upper(), logging.DEBUG)

logger.setLevel(logging_level)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create and configure handler
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)

# Prevent propagation to root logger
logger.propagate = False
