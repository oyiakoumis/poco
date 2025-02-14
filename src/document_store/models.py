"""Pydantic models for the document store module."""

from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from pydantic import BaseModel, Field, field_validator

from document_store.exceptions import InvalidDatasetSchemaError
from document_store.types import DatasetSchema, PydanticObjectId, RecordData
from document_store.validators.schema import validate_schema


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
