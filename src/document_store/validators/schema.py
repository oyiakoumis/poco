"""Schema validation module for dataset schemas."""

from typing import Dict, List, Optional, Set, Tuple

from document_store.exceptions import (
    InvalidDatasetSchemaError,
    InvalidSchemaUpdateError,
    TypeConversionError,
)
from document_store.types import DatasetSchema, FieldType, SchemaField, SAFE_TYPE_CONVERSIONS
from document_store.validators.factory import get_validator


def validate_schema_update(old_schema: DatasetSchema, new_schema: DatasetSchema) -> None:
    """Validate schema update operation.

    Args:
        old_schema: Current schema
        new_schema: Proposed new schema

    Raises:
        InvalidSchemaUpdateError: If the schema update is invalid
    """
    # Check for duplicate field names in new schema
    field_names = [field.field_name for field in new_schema]
    if len(field_names) != len(set(field_names)):
        raise InvalidSchemaUpdateError("Duplicate field names in new schema")

    # Create field maps for easy lookup
    old_fields = {field.field_name: field for field in old_schema}
    new_fields = {field.field_name: field for field in new_schema}

    # Check type changes for existing fields
    for field_name, new_field in new_fields.items():
        if field_name in old_fields:
            old_field = old_fields[field_name]
            if old_field.type != new_field.type:
                # Check if type conversion is allowed
                allowed_conversions = SAFE_TYPE_CONVERSIONS.get(old_field.type, set())
                if new_field.type not in allowed_conversions:
                    raise InvalidSchemaUpdateError(
                        f"Cannot convert field '{field_name}' from {old_field.type} to {new_field.type}"
                    )

def validate_schema(schema: DatasetSchema) -> DatasetSchema:
    """Validate dataset schema.

    Args:
        schema: Schema to validate

    Returns:
        Validated schema

    Raises:
        InvalidDatasetSchemaError: If schema is invalid
    """
    # Check for duplicate field names
    field_names = [field.field_name for field in schema]
    if len(field_names) != len(set(field_names)):
        raise InvalidDatasetSchemaError("Duplicate field names in schema")

    # Validate field types and default values
    for field in schema:
        if not isinstance(field.type, FieldType):
            try:
                field.type = FieldType(field.type)
            except ValueError:
                raise InvalidDatasetSchemaError(f"Invalid field type: {field.type}")

        validator = get_validator(field.type)
        if field.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
            if not field.options:
                raise InvalidDatasetSchemaError(f"Options not provided for {field.type} field '{field.field_name}'")
            validator.set_options(field.options)

        # Validate default value using appropriate validator
        if field.default is not None:
            try:
                field.default = validator.validate_default(field.default)
            except ValueError as e:
                raise InvalidDatasetSchemaError(f"Invalid default value for field '{field.field_name}': {str(e)}")

    return schema
