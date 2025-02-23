"""Dataset model and related utilities."""

from document_store.models.base import BaseDocument
from document_store.models.schema import DatasetSchema


class Dataset(BaseDocument):
    """Dataset model representing a collection of records with a defined schema."""

    name: str
    description: str
    dataset_schema: DatasetSchema
