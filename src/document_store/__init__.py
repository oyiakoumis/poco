"""Document store package."""

from document_store.dataset_manager import DatasetManager
from document_store.exceptions import (
    DatabaseError,
    DatasetNameExistsError,
    DatasetNotFoundError,
    DocumentStoreError,
    InvalidFieldTypeError,
    InvalidFieldValueError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from document_store.models import Dataset, Record
from document_store.types import SchemaField, FieldType

__all__ = [
    # Main class
    "DatasetManager",
    # Models
    "Dataset",
    "Record",
    "SchemaField",
    # Types
    "FieldType",
    # Exceptions
    "DocumentStoreError",
    "DatasetNameExistsError",
    "DatasetNotFoundError",
    "DatabaseError",
    "InvalidFieldTypeError",
    "InvalidFieldValueError",
    "InvalidRecordDataError",
    "RecordNotFoundError",
]
