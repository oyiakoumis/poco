"""Type definitions for the document store module."""

from datetime import date, datetime
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
    DATE = "date"
    DATETIME = "datetime"
    SELECT = "select"
    MULTI_SELECT = "multi_select"


class SchemaField(BaseModel):
    """Field definition for dataset schema."""

    field_name: str = Field(description="Name of the field in the dataset schema", min_length=1, max_length=100, json_schema_extra={"examples": ["user_age"]})
    description: str = Field(
        description="Detailed description of what this field represents",
        min_length=1,
        max_length=500,
        json_schema_extra={"examples": ["Age of the user in years"]},
    )
    type: FieldType = Field(description="Data type of the field", json_schema_extra={"examples": [FieldType.INTEGER]})
    required: bool = Field(default=False, description="Whether this field must be present in all records", json_schema_extra={"examples": [True]})
    default: Optional[Any] = Field(default=None, description="Default value for the field if not provided", json_schema_extra={"examples": [0]})
    options: Optional[List[str]] = Field(
        default=None, description="List of allowed values for select/multi-select fields", json_schema_extra={"examples": [["option1", "option2"]]}
    )

    model_config = {"arbitrary_types_allowed": True, "populate_by_name": True, "from_attributes": True}


# Type definitions for better readability
DatasetSchema = List[SchemaField]
RecordData = Dict[str, Any]
