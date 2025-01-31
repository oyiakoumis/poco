from datetime import datetime
from enum import Enum
from typing import Any, Dict

from database_manager.exceptions import ValidationError


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
        name: str,
        description: str,
        field_type: DataType,
        required: bool = False,
        default: Any = None,
    ):
        self.name = name
        self.description = description
        self.field_type = field_type
        self.required = required
        self.default = default

    def validate(self, value: Any) -> bool:
        if value is None:
            if self.required:
                raise ValidationError(f"Field {self.name} is required")
            return True

        type_checks = {
            DataType.STRING: str,
            DataType.INTEGER: int,
            DataType.FLOAT: (int, float),
            DataType.BOOLEAN: bool,
            DataType.DATETIME: datetime,
        }

        expected_type = type_checks[self.field_type]
        if not isinstance(value, expected_type):
            raise ValidationError(f"Field {self.name} expected {self.field_type.value}, got {type(value)}")

        return True

    def to_dict(self) -> Dict:
        return {"name": self.name, "description": self.description, "field_type": self.field_type.value, "required": self.required, "default": self.default}

    @classmethod
    def from_dict(cls, data: Dict) -> "SchemaField":
        return cls(name=data["name"], description=data["description"], field_type=DataType(data["field_type"]), required=data["required"], default=data["default"])
