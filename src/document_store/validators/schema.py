"""Schema validation module for dataset schemas."""

from typing import Dict, List, Optional, Set

from document_store.exceptions import (
    InvalidDatasetSchemaError,
    InvalidSchemaUpdateError,
    TypeConversionError,
)
from document_store.types import DatasetSchema, FieldType, SchemaField, SAFE_TYPE_CONVERSIONS
from document_store.validators.factory import get_validator


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
                raise InvalidDatasetSchemaError(
                    f"Options not provided for {field.type} field '{field.field_name}'"
                )
            validator.set_options(field.options)

        # Validate default value using appropriate validator
        if field.default is not None:
            try:
                field.default = validator.validate_default(field.default)
            except ValueError as e:
                raise InvalidDatasetSchemaError(
                    f"Invalid default value for field '{field.field_name}': {str(e)}"
                )

    return schema


def validate_schema_update(current_schema: DatasetSchema, new_schema: DatasetSchema) -> None:
    """Validate schema update operation.
    
    Args:
        current_schema: Current dataset schema
        new_schema: New schema to validate
        
    Raises:
        InvalidSchemaUpdateError: If schema update is invalid
    """
    # Create field name to field mappings
    current_fields: Dict[str, SchemaField] = {
        field.field_name: field for field in current_schema
    }
    new_fields: Dict[str, SchemaField] = {
        field.field_name: field for field in new_schema
    }

    # Check for invalid type conversions
    for field_name, new_field in new_fields.items():
        if field_name in current_fields:
            current_field = current_fields[field_name]
            
            # Skip if type hasn't changed
            if current_field.type == new_field.type:
                continue
                
            # Check if type conversion is allowed
            if current_field.type not in SAFE_TYPE_CONVERSIONS:
                raise InvalidSchemaUpdateError(
                    f"Cannot convert field '{field_name}' from type {current_field.type}"
                )
                
            allowed_conversions = SAFE_TYPE_CONVERSIONS[current_field.type]
            if new_field.type not in allowed_conversions:
                raise InvalidSchemaUpdateError(
                    f"Cannot convert field '{field_name}' from {current_field.type} to {new_field.type}"
                )

    # Validate new fields have default values if required
    for field_name, new_field in new_fields.items():
        if (
            field_name not in current_fields  # New field
            and new_field.required  # Is required
            and new_field.default is None  # No default value
        ):
            raise InvalidSchemaUpdateError(
                f"New required field '{field_name}' must have a default value"
            )

    # Validate the complete new schema
    validate_schema(new_schema)
