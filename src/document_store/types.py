"""Type definitions for the document store module."""
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class FieldType(str, Enum):
    """Supported field types for dataset structure."""
    INTEGER = "int"
    FLOAT = "float"
    STRING = "str"


class Field(BaseModel):
    """Field definition for dataset structure."""
    field_name: str
    description: str
    type: FieldType
    required: bool = False
    default: Optional[Any] = None


class DatasetStructure(BaseModel):
    """Dataset structure definition."""
    fields: List[Field]


# Type aliases for better code readability
DatasetId = str
RecordId = str
UserId = str
RecordData = Dict[str, Any]
QueryFilter = Dict[str, Any]

# Type for values that can be stored in a record
FieldValue = Union[int, float, str]
