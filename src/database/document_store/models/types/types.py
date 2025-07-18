"""Type implementations for different field types."""

from datetime import date, datetime
from typing import Any, List, Optional, Set

from database.document_store.models.types.base import BaseType
from database.document_store.models.types.constants import FieldType


class BooleanType(BaseType):
    """Boolean type implementation."""

    field_type = FieldType.BOOLEAN

    def validate(self, value: Any) -> bool:
        """Validate and convert to boolean."""
        if isinstance(value, str):
            value = value.lower()
            if value in ("true", "1", "yes"):
                return True
            if value in ("false", "0", "no"):
                return False
            raise ValueError(f"Cannot convert string '{value}' to boolean")
        return bool(value)

    def validate_default(self, value: Any) -> Optional[bool]:
        """Validate and convert default value to boolean."""
        if value is None:
            return None
        return self.validate(value)


class IntegerType(BaseType):
    """Integer type implementation."""

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


class FloatType(BaseType):
    """Float type implementation."""

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


class StringType(BaseType):
    """String type implementation."""

    field_type = FieldType.STRING

    def validate(self, value: Any) -> str:
        """Validate and convert to string."""
        return str(value)

    def validate_default(self, value: Any) -> Optional[str]:
        """Validate and convert default value to string."""
        if value is None:
            return None
        return self.validate(value)


class DateType(BaseType):
    """Date type implementation."""

    field_type = FieldType.DATE

    def validate(self, value: Any) -> datetime:
        """Validate and convert to datetime at midnight UTC.

        Accepts:
        - date objects
        - datetime objects
        - strings in YYYY-MM-DD format
        """
        if isinstance(value, datetime):
            return value.replace(hour=0, minute=0, second=0, microsecond=0)
        if isinstance(value, date):
            if isinstance(value, datetime):  # Handle datetime subclass
                return value.replace(hour=0, minute=0, second=0, microsecond=0)
            return datetime(value.year, value.month, value.day)
        if isinstance(value, str):
            try:
                parsed_date = datetime.strptime(value, "%Y-%m-%d")
                return parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
            except ValueError:
                raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {value}")
        raise ValueError(f"Cannot convert {type(value)} to date")

    def validate_default(self, value: Any) -> Optional[datetime]:
        """Validate and convert default value to date."""
        if value is None:
            return None
        return self.validate(value)


class DateTimeType(BaseType):
    """DateTime type implementation."""

    field_type = FieldType.DATETIME

    def validate(self, value: Any) -> datetime:
        """Validate and convert to datetime.

        Accepts:
        - datetime objects
        - strings in YYYY-MM-DD[T ]HH:MM:SS format
        """
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                try:
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    raise ValueError("Invalid datetime format. Expected YYYY-MM-DD[T ]HH:MM:SS")
        raise ValueError(f"Cannot convert {type(value)} to datetime")

    def validate_default(self, value: Any) -> Optional[datetime]:
        """Validate and convert default value to datetime."""
        if value is None:
            return None
        return self.validate(value)


class SelectType(BaseType):
    """Select type implementation."""

    field_type = FieldType.SELECT

    def __init__(self):
        """Initialize select type."""
        self._options: Optional[Set[str]] = None

    def set_options(self, options: List[str]) -> None:
        """Set allowed options for the field.

        Args:
            options: List of allowed values
        """
        self._options = set(options)

    def validate(self, value: Any) -> str:
        """Validate and convert to allowed string value."""
        if self._options is None:
            raise ValueError("Options not set for select field")

        str_value = str(value)
        if str_value not in self._options:
            raise ValueError(f"Value must be one of: {', '.join(sorted(self._options))}")

        return str_value

    def validate_default(self, value: Any) -> Optional[str]:
        """Validate and convert default value to string."""
        if value is None:
            return None
        return self.validate(value)


class MultiSelectType(BaseType):
    """MultiSelect type implementation."""

    field_type = FieldType.MULTI_SELECT

    def __init__(self):
        """Initialize multi-select type."""
        self._options: Optional[Set[str]] = None

    def set_options(self, options: List[str]) -> None:
        """Set allowed options for the field.

        Args:
            options: List of allowed values
        """
        self._options = set(options)

    def validate(self, value: Any) -> List[str]:
        """Validate and convert to list of allowed string values."""
        if self._options is None:
            raise ValueError("Options not set for multi-select field")

        if isinstance(value, str):
            # Handle empty string case
            if not value:
                return []
            values = [v.strip() for v in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            values = [str(v) for v in value]
        else:
            raise ValueError("Value must be string (comma-separated) or list/tuple/set")

        # Convert to set for validation, then back to list for consistent order
        value_set = set(values)
        invalid = value_set - self._options
        if invalid:
            raise ValueError(f"Invalid options: {', '.join(sorted(invalid))}. Must be from: {', '.join(sorted(self._options))}")

        return sorted(values)  # Sort for consistent order

    def validate_default(self, value: Any) -> Optional[List[str]]:
        """Validate and convert default value to list of strings."""
        if value is None:
            return None
        return self.validate(value)
