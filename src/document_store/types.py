"""Type definitions for the document store module."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from bson import ObjectId
from pydantic import BaseModel, Field
from pydantic_core import core_schema


class PydanticObjectId(ObjectId):
    """ObjectId field for Pydantic models."""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(ObjectId),
                    core_schema.chain_schema(
                        [
                            core_schema.str_schema(),
                            core_schema.no_info_plain_validator_function(cls.validate),
                        ]
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(lambda x: str(x), when_used="json"),
        )

    @classmethod
    def validate(cls, v):
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
    BOOLEAN = "bool"


class SchemaField(BaseModel):
    """Field definition for dataset schema."""

    field_name: str = Field(description="Name of the field in the dataset schema", min_length=1, max_length=100, example="user_age")
    description: str = Field(description="Detailed description of what this field represents", min_length=1, max_length=500, example="Age of the user in years")
    type: FieldType = Field(description="Data type of the field", example=FieldType.INTEGER)
    required: bool = Field(default=False, description="Whether this field must be present in all records", example=True)
    default: Optional[Any] = Field(default=None, description="Default value for the field if not provided", example=0)

    model_config = {"arbitrary_types_allowed": True, "populate_by_name": True, "from_attributes": True}


DatasetSchema = List[SchemaField]


# Type aliases for better code readability
DatasetId = str
RecordId = str
UserId = str
RecordData = Dict[str, Any]
QueryFilter = Dict[str, Any]

# Type for values that can be stored in a record
FieldValue = Union[int, float, str]
