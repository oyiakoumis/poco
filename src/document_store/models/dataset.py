"""Dataset model and related utilities."""

from pydantic import field_validator

from document_store.models.base import BaseDocument
from document_store.models.schema import DatasetSchema
from document_store.validators.schema import validate_schema


class Dataset(BaseDocument):
    """Dataset model representing a collection of records with a defined schema."""

    name: str
    description: str
    dataset_schema: DatasetSchema

    @field_validator("dataset_schema")
    @classmethod
    def validate_schema(cls, schema: DatasetSchema) -> DatasetSchema:
        """Validate dataset schema."""
        return validate_schema(schema)
