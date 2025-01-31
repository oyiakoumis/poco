from datetime import datetime
from typing import Any
from enum import Enum

from database.exceptions import ValidationError


class DataType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    # Keeping only basic types for v1


class SchemaField:
    def __init__(
        self,
        field_type: DataType,
        required: bool = False,
        default: Any = None,
    ):
        self.field_type = field_type
        self.required = required
        self.default = default

    def validate(self, value: Any) -> bool:
        if value is None:
            if self.required:
                raise ValidationError(f"Field is required")
            return True

        # Basic type validation
        type_checks = {
            DataType.STRING: str,
            DataType.INTEGER: int,
            DataType.FLOAT: (int, float),
            DataType.BOOLEAN: bool,
            DataType.DATETIME: datetime,
        }

        expected_type = type_checks[self.field_type]
        if not isinstance(value, expected_type):
            raise ValidationError(f"Expected {self.field_type.value}, got {type(value)}")

        return True


from pymongo import MongoClient
