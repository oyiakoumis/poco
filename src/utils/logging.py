"""Logging utilities for the application."""

import logging
import sys

from constants import LOGGING_LEVEL

# Create and configure application logger
logger = logging.getLogger("poco")
logger.setLevel(LOGGING_LEVEL)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create and configure handler
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)

# Prevent propagation to root logger
logger.propagate = False
