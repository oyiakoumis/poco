"""Type definitions for the document store module."""

from enum import Enum
from typing import Any

from bson import ObjectId
from pydantic_core import core_schema


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


# Safe type conversion mappings
SAFE_TYPE_CONVERSIONS = {
    FieldType.INTEGER: {FieldType.FLOAT, FieldType.STRING},
    FieldType.FLOAT: {FieldType.STRING},
    FieldType.BOOLEAN: {FieldType.STRING},
    FieldType.DATE: {FieldType.STRING, FieldType.DATETIME},
    FieldType.DATETIME: {FieldType.STRING},
    # SELECT and MULTI_SELECT only allow same type conversions
    FieldType.SELECT: {FieldType.SELECT},
    FieldType.MULTI_SELECT: {FieldType.MULTI_SELECT},
}
