"""Field validation utilities."""

from typing import Optional, Tuple

from document_store.exceptions import InvalidDatasetSchemaError, InvalidRecordDataError
from document_store.models.dataset import Dataset
from document_store.models.field import SchemaField
from document_store.models.schema import DatasetSchema
from document_store.models.types import SAFE_TYPE_CONVERSIONS
from document_store.validators.schema import validate_schema


def validate_field_update(dataset: Dataset, field_name: str, field_update: SchemaField) -> Tuple[Optional[SchemaField], Optional[DatasetSchema]]:
    """Validates field update and returns old field and new schema.

    Args:
        dataset: Current dataset
        field_name: Name of field to update
        field_update: New field definition

    Returns:
        Tuple of (old field, new schema), or (None, None) if no changes needed

    Raises:
        InvalidDatasetSchemaError: If field not found or update invalid
        InvalidRecordDataError: If type conversion not safe
    """
    old_field = None
    new_schema = []

    # Find and validate field
    for field in dataset.dataset_schema:
        if field.field_name == field_name:
            old_field = field
            # Check if anything changed
            if (
                field.type == field_update.type
                and field.description == field_update.description
                and field.required == field_update.required
                and field.default == field_update.default
                and field.options == field_update.options
            ):
                # No changes needed
                return None, None

            # Validate type conversion if type is changing
            if field.type != field_update.type:
                safe_conversions = SAFE_TYPE_CONVERSIONS.get(field.type, set())
                if field_update.type not in safe_conversions:
                    raise InvalidRecordDataError(f"Cannot safely convert field '{field_name}' from {field.type} " f"to {field_update.type}")
            new_schema.append(field_update)
        else:
            new_schema.append(field)

    if not old_field:
        raise InvalidDatasetSchemaError(f"Field '{field_name}' not found in schema")

    # Validate complete schema
    validate_schema(new_schema)
    return old_field, new_schema
