from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict

from database_manager.exceptions import ValidationError


class DataType(Enum):
    """
    Enumeration of supported data types.
    """

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    # Keeping only basic types for v1


@dataclass
class SchemaField:
    """
    Represents a field in a collection's schema.
    """

    name: str
    description: str
    field_type: DataType
    required: bool = False
    default: Any = None

    def validate(self, value: Any) -> bool:
        """
        Validate a value against the field's type and requirements.
        """
        if value is None:
            if self.required:
                raise ValidationError(f"Field '{self.name}' is required")
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
            raise ValidationError(f"Field '{self.name}' expected type '{self.field_type.value}', got {type(value).__name__}")

        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the schema field to a dictionary.
        """
        return {"name": self.name, "description": self.description, "field_type": self.field_type.value, "required": self.required, "default": self.default}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SchemaField:
        """
        Create a SchemaField instance from a dictionary.
        """
        return cls(
            name=data["name"],
            description=data["description"],
            field_type=DataType(data["field_type"]),
            required=data.get("required", False),
            default=data.get("default"),
        )
