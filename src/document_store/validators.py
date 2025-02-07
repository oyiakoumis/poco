"""Validators for the document store module."""

from typing import Any, Dict, List, Optional

from .exceptions import InvalidFieldValueError, InvalidRecordDataError
from .types import DatasetSchema, SchemaField, FieldType


def validate_record_data(data: Dict[str, Any], schema: DatasetSchema) -> Dict[str, Any]:
    """
    Validate record data against dataset schema.

    Args:
        data: Record data to validate
        schema: Dataset schema to validate against

    Returns:
        Validated and type-converted data

    Raises:
        InvalidRecordDataError: If data doesn't match schema
        InvalidFieldValueError: If field value doesn't match type
    """
    validated_data = {}
    field_map = {field.field_name: field for field in schema}

    # Check for unknown fields
    unknown_fields = set(data.keys()) - set(field_map.keys())
    if unknown_fields:
        raise InvalidRecordDataError(f"Unknown fields in record data: {', '.join(unknown_fields)}")

    # Check required fields and validate types
    for field in schema:
        value = data.get(field.field_name)

        # Handle required fields
        if field.required and value is None:
            if field.default is not None:
                value = field.default
            else:
                raise InvalidRecordDataError(f"Required field '{field.field_name}' is missing")

        # Skip optional fields with no value
        if value is None:
            if field.default is not None:
                validated_data[field.field_name] = field.default
            continue

        # Validate and convert field value
        try:
            if field.type == FieldType.INTEGER:
                validated_data[field.field_name] = int(value)
            elif field.type == FieldType.FLOAT:
                validated_data[field.field_name] = float(value)
            elif field.type == FieldType.STRING:
                validated_data[field.field_name] = str(value)
        except (ValueError, TypeError):
            raise InvalidFieldValueError(f"Invalid value for field '{field.field_name}': " f"expected {field.type.value}, got {type(value).__name__}")

    return validated_data


def validate_query_fields(query: Dict[str, Any], schema: DatasetSchema) -> None:
    """
    Validate that query fields exist in dataset schema.

    Args:
        query: Query dictionary
        schema: Dataset schema to validate against

    Raises:
        InvalidRecordDataError: If query contains unknown fields
    """
    field_names = {field.field_name for field in schema}
    query_fields = set(query.keys())
    unknown_fields = query_fields - field_names

    if unknown_fields:
        raise InvalidRecordDataError(f"Query contains unknown fields: {', '.join(unknown_fields)}")
