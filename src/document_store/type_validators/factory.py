"""Factory for creating type validators."""

from typing import Dict, Type

from document_store.models.types import FieldType
from document_store.type_validators.base import TypeValidator
from document_store.type_validators.type_validators import (
    BooleanValidator,
    DateTimeValidator,
    DateValidator,
    FloatValidator,
    IntegerValidator,
    MultiSelectValidator,
    SelectValidator,
    StringValidator,
)


class ValidatorFactory:
    """Factory for creating type validators."""

    _validators: Dict[str, Type[TypeValidator]] = {
        FieldType.INTEGER.value: IntegerValidator,
        FieldType.FLOAT.value: FloatValidator,
        FieldType.STRING.value: StringValidator,
        FieldType.BOOLEAN.value: BooleanValidator,
        FieldType.DATE.value: DateValidator,
        FieldType.DATETIME.value: DateTimeValidator,
        FieldType.SELECT.value: SelectValidator,
        FieldType.MULTI_SELECT.value: MultiSelectValidator,
    }

    @classmethod
    def register_validator(cls, validator_class: Type[TypeValidator]) -> None:
        """Register a new validator class.

        Args:
            validator_class: Validator class to register
        """
        cls._validators[validator_class.get_field_type().value] = validator_class

    @classmethod
    def get_validator(cls, field_type: FieldType) -> TypeValidator:
        """Get a validator instance for a field type.

        Args:
            field_type: Field type to get validator for

        Returns:
            Validator instance

        Raises:
            ValueError: If no validator exists for the field type
        """
        validator_class = cls._validators.get(getattr(field_type, "value", None))
        if not validator_class:
            raise ValueError(f"No validator registered for field type: {field_type}")
        return validator_class()


# Convenience function
def get_validator(field_type: FieldType) -> TypeValidator:
    """Get a validator instance for a field type."""
    return ValidatorFactory.get_validator(field_type)
