"""Base type class for field type implementations."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from database.document_store.models.types.constants import (
    SAFE_TYPE_CONVERSIONS,
    VALID_AGGREGATIONS,
    AggregationType,
    FieldType,
)


class BaseType(ABC):
    """Base class for all field types."""

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

    def can_convert_from(self, other_type: FieldType) -> bool:
        """Check if this type can safely convert from another type.

        Args:
            other_type: Type to convert from

        Returns:
            True if conversion is safe, False otherwise
        """
        return other_type in SAFE_TYPE_CONVERSIONS.get(self.field_type, set())

    def can_aggregate(self, operation: AggregationType) -> bool:
        """Check if this type supports a given aggregation operation.

        Args:
            operation: Aggregation operation to check

        Returns:
            True if the operation is supported, False otherwise
        """
        return operation in VALID_AGGREGATIONS.get(self.field_type, set())

    @classmethod
    def get_field_type(cls) -> FieldType:
        """Get the field type this implementation handles."""
        return cls.field_type
