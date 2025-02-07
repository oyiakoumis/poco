"""Type definitions for the document store module."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from bson import ObjectId
from pydantic import BaseModel, Field


class PydanticObjectId(ObjectId):
    """ObjectId field for Pydantic models."""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, handler):
        if isinstance(v, ObjectId):
            return v
        if isinstance(v, str):
            try:
                return ObjectId(v)
            except Exception:
                raise ValueError("Invalid ObjectId format")
        raise ValueError("Invalid ObjectId")


class FieldType(str, Enum):
    """Supported field types for dataset schema."""

    INTEGER = "int"
    FLOAT = "float"
    STRING = "str"


class SchemaField(BaseModel):
    """Field definition for dataset schema."""

    field_name: str = Field(description="Name of the field in the dataset schema", min_length=1, max_length=100, example="user_age")
    description: str = Field(description="Detailed description of what this field represents", min_length=1, max_length=500, example="Age of the user in years")
    type: FieldType = Field(description="Data type of the field", example=FieldType.INTEGER)
    required: bool = Field(default=False, description="Whether this field must be present in all records", example=True)
    default: Optional[Any] = Field(default=None, description="Default value for the field if not provided", example=0)


DatasetSchema = List[SchemaField]


# Type aliases for better code readability
DatasetId = str
RecordId = str
UserId = str
RecordData = Dict[str, Any]
QueryFilter = Dict[str, Any]

# Type for values that can be stored in a record
FieldValue = Union[int, float, str]
