"""Pydantic models for the document store module."""

from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from pydantic import BaseModel, Field, field_validator

from document_store.exceptions import InvalidDatasetSchemaError, InvalidFieldTypeError
from document_store.types import DatasetSchema, FieldType, PydanticObjectId, RecordData
from document_store.validators.factory import get_validator


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
                    raise InvalidFieldTypeError(f"Invalid default value for field '{field.field_name}': {str(e)}")

        return schema

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
