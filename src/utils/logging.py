"""Logging utilities for the application."""

import logging
import os
import sys

# Import OpenTelemetry components for Azure Monitor
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry.instrumentation.logging import LoggingInstrumentor

# Create and configure application logger
logger = logging.getLogger("poco")

logging_level = getattr(logging, os.environ.get("LOGGING_LEVEL", "DEBUG").upper(), logging.DEBUG)

logger.setLevel(logging_level)

# Create formatter with process and thread IDs for worker identification
formatter = logging.Formatter("%(asctime)s - PID:%(process)d - Thread:%(thread)d - %(name)s - %(levelname)s - %(message)s")

# Create and configure stdout handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# Add stdout handler to logger
logger.addHandler(console_handler)

# Configure Azure Monitor if connection string is available
appinsights_connection_string = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
if appinsights_connection_string:
    # Initialize Azure Monitor OpenTelemetry exporter
    configure_azure_monitor(
        connection_string=appinsights_connection_string,
    )

    # Instrument the logging system to send logs to Azure Monitor
    LoggingInstrumentor().instrument(level=logging_level, excluded_loggers=["azure"])  # Avoid recursive logging

# Prevent propagation to root logger to avoid duplicate logs
logger.propagate = False
