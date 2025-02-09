"""Base validator class for type validation."""

from abc import ABC, abstractmethod
from typing import Any, Optional, Type

from document_store.types import FieldType


class TypeValidator(ABC):
    """Base class for type validators."""

    field_type: FieldType

    @abstractmethod
    def validate(self, value: Any) -> Any:
        """Validate and convert a value to the correct type.

        Args:
            value: Value to validate and convert

        Returns:
            Converted value

        Raises:
            ValueError: If value cannot be converted to the correct type
        """
        pass

    @abstractmethod
    def validate_default(self, value: Any) -> Optional[Any]:
        """Validate and convert a default value.

        Args:
            value: Default value to validate and convert

        Returns:
            Converted default value or None if value is None

        Raises:
            ValueError: If value cannot be converted to the correct type
        """
        pass

    @classmethod
    def get_field_type(cls) -> FieldType:
        """Get the field type this validator handles."""
        return cls.field_type
