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
    InvalidSchemaUpdateError,
    RecordNotFoundError,
    TypeConversionError,
)
from document_store.models import Dataset, Record

__all__ = [
    # Main class
    "DatasetManager",
    # Models
    "Dataset",
    "Record",
    # Exceptions
    "DocumentStoreError",
    "DatasetNameExistsError",
    "DatasetNotFoundError",
    "DatabaseError",
    "InvalidFieldTypeError",
    "InvalidFieldValueError",
    "InvalidRecordDataError",
    "InvalidSchemaUpdateError",
    "RecordNotFoundError",
    "TypeConversionError",
]
