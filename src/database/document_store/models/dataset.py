"""Dataset model and related utilities."""

from database.document_store.models.schema import DatasetSchema
from models.base import BaseDocument


class Dataset(BaseDocument):
    """Dataset model representing a collection of records with a defined schema."""

    name: str
    description: str
    dataset_schema: DatasetSchema
