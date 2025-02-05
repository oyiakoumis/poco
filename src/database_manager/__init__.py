"""Database management package."""

from .document_db import DocumentDB
from .exceptions import (
    CollectionNotFoundError,
    DocumentNotFoundError,
    SchemaValidationError,
)

__all__ = [
    "DocumentDB",
    "CollectionNotFoundError",
    "DocumentNotFoundError",
    "SchemaValidationError",
]
