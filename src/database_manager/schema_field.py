from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from database_manager.exceptions import ValidationError


class FieldType(Enum):
    """Supported data types for schema fields."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    SELECT = "select"
    MULTI_SELECT = "multi_select"


@dataclass
class SchemaField:
    """
    Represents a field in a collection's schema.

    Attributes:
        name: Field name
        description: Field description
        field_type: Type of data this field accepts
        required: Whether this field must have a value
        default: Default value if none is provided
        options: Available options for SELECT and MULTI_SELECT types
    """

    name: str
    description: str
    field_type: FieldType
    required: bool = False
    default: Any = None
    options: Optional[List[str]] = None

    def __post_init__(self):
        """Validate the field configuration."""
        if self.field_type in (FieldType.SELECT, FieldType.MULTI_SELECT):
            if not self.options:
                raise ValueError(f"Field '{self.name}' of type {self.field_type.value} must have options defined")
            self.options = list(set(str(opt) for opt in self.options))

        if self.default is not None:
            self.validate(self.default)

    def validate(self, value: Any) -> bool:
        """Validate a value against the field's type and requirements."""
        if value is None:
            if self.required:
                raise ValidationError(f"Field '{self.name}' is required")
            return True

        # Type validation
        type_checks = {
            FieldType.STRING: str,
            FieldType.INTEGER: int,
            FieldType.FLOAT: (int, float),
            FieldType.BOOLEAN: bool,
            FieldType.DATETIME: datetime,
            FieldType.SELECT: str,
            FieldType.MULTI_SELECT: (list, tuple, set),
        }

        expected_type = type_checks[self.field_type]
        if not isinstance(value, expected_type):
            raise ValidationError(f"Field '{self.name}' expected type '{self.field_type.value}', " f"got {type(value).__name__}")

        # Select validation
        if self.field_type == FieldType.SELECT and value not in self.options:
            raise ValidationError(f"Field '{self.name}' value must be one of: {', '.join(self.options)}")

        # Multi-select validation
        if self.field_type == FieldType.MULTI_SELECT:
            invalid = set(str(v) for v in value) - set(self.options)
            if invalid:
                raise ValidationError(f"Field '{self.name}' contains invalid options: {', '.join(invalid)}")

        return True

    def to_dict(self) -> Dict[str, Any]:
        """Convert the schema field to a dictionary."""
        result = {"name": self.name, "description": self.description, "field_type": self.field_type.value, "required": self.required, "default": self.default}
        if self.options is not None:
            result["options"] = self.options
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SchemaField:
        """Create a SchemaField instance from a dictionary."""
        return cls(
            name=data["name"],
            description=data["description"],
            field_type=FieldType(data["field_type"]),
            required=data.get("required", False),
            default=data.get("default"),
            options=data.get("options"),
        )


if __name__ == "__main__":
    # Create a status field
    status = SchemaField(name="status", description="Current status", field_type=FieldType.SELECT, options=["open", "closed"], default="open")

    # Create a tags field
    tags = SchemaField(name="tags", description="Issue tags", field_type=FieldType.MULTI_SELECT, options=["bug", "feature", "docs"])

    # Validate values
    status.validate("open")  # OK
    status.validate("invalid")  # Raises ValidationError
    tags.validate(["bug", "docs"])  # OK
    tags.validate(["bug", "invalid"])  # Raises ValidationError
