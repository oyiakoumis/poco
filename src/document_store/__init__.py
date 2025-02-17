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
from document_store.types import SAFE_TYPE_CONVERSIONS, DatasetSchema, FieldType, SchemaField

__all__ = [
    # Main class
    "DatasetManager",
    # Models
    "Dataset",
    "Record",
    "SchemaField",
    # Types
    "DatasetSchema",
    "FieldType",
    "SAFE_TYPE_CONVERSIONS",
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
