"""Pydantic models for the document store module."""

from datetime import datetime, timezone
from typing import List, Optional, Tuple

from bson import ObjectId
from pydantic import BaseModel, Field, field_validator

from document_store.exceptions import InvalidDatasetSchemaError, InvalidRecordDataError
from document_store.types import DatasetSchema, FieldType, PydanticObjectId, RecordData, SchemaField, SAFE_TYPE_CONVERSIONS
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
                raise InvalidDatasetSchemaError(f"Options not provided for {field.type} field '{field.field_name}'")
            validator.set_options(field.options)

        # Validate default value using appropriate validator
        if field.default is not None:
            try:
                field.default = validator.validate_default(field.default)
            except ValueError as e:
                raise InvalidDatasetSchemaError(f"Invalid default value for field '{field.field_name}': {str(e)}")

    return schema


def validate_field_update(
    dataset: "Dataset", 
    field_name: str, 
    field_update: SchemaField
) -> Tuple[Optional[SchemaField], Optional[List[SchemaField]]]:
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
            if (field.type == field_update.type and 
                field.description == field_update.description and
                field.required == field_update.required and
                field.default == field_update.default and
                field.options == field_update.options):
                # No changes needed
                return None, None
                
            # Validate type conversion if type is changing
            if field.type != field_update.type:
                safe_conversions = SAFE_TYPE_CONVERSIONS.get(field.type, set())
                if field_update.type not in safe_conversions:
                    raise InvalidRecordDataError(
                        f"Cannot safely convert field '{field_name}' from {field.type} "
                        f"to {field_update.type}"
                    )
            new_schema.append(field_update)
        else:
            new_schema.append(field)
            
    if not old_field:
        raise InvalidDatasetSchemaError(f"Field '{field_name}' not found in schema")
        
    # Validate complete schema
    validate_schema(new_schema)
    return old_field, new_schema


class Dataset(BaseModel):
    """Dataset model representing a collection of records with a defined schema."""

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    user_id: str
    name: str
    description: str
    dataset_schema: DatasetSchema
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("dataset_schema")
    @classmethod
    def validate_schema(cls, schema: DatasetSchema) -> DatasetSchema:
        """Validate dataset schema."""
        return validate_schema(schema)

    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True, "from_attributes": True}


class Record(BaseModel):
    """Record model representing a single document in a dataset."""

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    user_id: str
    dataset_id: PydanticObjectId
    data: RecordData
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"populate_by_name": True}
