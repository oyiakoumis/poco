"""Concrete validator implementations for different field types."""

from typing import Any, Optional

from document_store.types import FieldType
from document_store.validators.base import TypeValidator


class IntegerValidator(TypeValidator):
    """Validator for integer fields."""

    field_type = FieldType.INTEGER

    def validate(self, value: Any) -> int:
        """Validate and convert to integer."""
        if isinstance(value, bool):
            raise ValueError("Boolean values cannot be converted to integer")
        return int(value)

    def validate_default(self, value: Any) -> Optional[int]:
        """Validate and convert default value to integer."""
        if value is None:
            return None
        return self.validate(value)


class FloatValidator(TypeValidator):
    """Validator for float fields."""

    field_type = FieldType.FLOAT

    def validate(self, value: Any) -> float:
        """Validate and convert to float."""
        if isinstance(value, bool):
            raise ValueError("Boolean values cannot be converted to float")
        return float(value)

    def validate_default(self, value: Any) -> Optional[float]:
        """Validate and convert default value to float."""
        if value is None:
            return None
        return self.validate(value)


class StringValidator(TypeValidator):
    """Validator for string fields."""

    field_type = FieldType.STRING

    def validate(self, value: Any) -> str:
        """Validate and convert to string."""
        return str(value)

    def validate_default(self, value: Any) -> Optional[str]:
        """Validate and convert default value to string."""
        if value is None:
            return None
        return self.validate(value)
