"""Document store package."""
from .exceptions import (
    DatasetNameExistsError,
    DatasetNotFoundError,
    DatabaseError,
    DocumentStoreError,
    InvalidFieldTypeError,
    InvalidFieldValueError,
    InvalidRecordDataError,
    RecordNotFoundError,
)
from .manager import DatasetManager
from .models import Dataset, Record
from .types import Field, FieldType

__all__ = [
    # Main class
    "DatasetManager",
    
    # Models
    "Dataset",
    "Record",
    "Field",
    
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
