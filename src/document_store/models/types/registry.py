"""Registry for type implementations."""

from typing import Dict, Type

from document_store.models.types.base import BaseType
from document_store.models.types.constants import FieldType
from document_store.models.types.types import (
    BooleanType,
    DateTimeType,
    DateType,
    FloatType,
    IntegerType,
    MultiSelectType,
    SelectType,
    StringType,
)


class TypeRegistry:
    """Registry for type implementations."""

    _types: Dict[str, Type[BaseType]] = {
        FieldType.INTEGER.value: IntegerType,
        FieldType.FLOAT.value: FloatType,
        FieldType.STRING.value: StringType,
        FieldType.BOOLEAN.value: BooleanType,
        FieldType.DATE.value: DateType,
        FieldType.DATETIME.value: DateTimeType,
        FieldType.SELECT.value: SelectType,
        FieldType.MULTI_SELECT.value: MultiSelectType,
    }

    @classmethod
    def register_type(cls, type_class: Type[BaseType]) -> None:
        """Register a new type implementation.

        Args:
            type_class: Type class to register
        """
        cls._types[type_class.get_field_type().value] = type_class

    @classmethod
    def get_type(cls, field_type: FieldType) -> BaseType:
        """Get a type instance for a field type.

        Args:
            field_type: Field type to get instance for

        Returns:
            Type instance

        Raises:
            ValueError: If no type exists for the field type
        """
        type_class = cls._types.get(getattr(field_type, "value", None))
        if not type_class:
            raise ValueError(f"No type registered for field type: {field_type}")
        return type_class()
