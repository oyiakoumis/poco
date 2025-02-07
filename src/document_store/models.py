"""Pydantic models for the document store module."""

from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from pydantic import BaseModel, Field, field_validator

from .exceptions import InvalidDatasetSchemaError, InvalidFieldTypeError
from .types import DatasetSchema
from .types import FieldType, PydanticObjectId, RecordData


class Dataset(BaseModel):
    """Dataset model representing a collection of records with a defined schema."""

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    user_id: str
    name: str
    description: str
    schema: DatasetSchema
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("schema")
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
                    raise InvalidFieldTypeError(f"Invalid field type: {field.type}")

            # Validate default value type if provided
            if field.default is not None:
                try:
                    if field.type == FieldType.INTEGER:
                        field.default = int(field.default)
                    elif field.type == FieldType.FLOAT:
                        field.default = float(field.default)
                    elif field.type == FieldType.STRING:
                        field.default = str(field.default)
                except (ValueError, TypeError):
                    raise InvalidFieldTypeError(f"Default value {field.default} does not match type {field.type}")

        return schema

    model_config = {"populate_by_name": True}


class Record(BaseModel):
    """Record model representing a single document in a dataset."""

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    user_id: str
    dataset_id: PydanticObjectId
    data: RecordData
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"populate_by_name": True}
