"""Dataset model and related utilities."""

from pydantic import Field
from database.document_store.models.schema import DatasetSchema
from models.base import BaseDocument


class Dataset(BaseDocument):
    """Dataset model representing a collection of records with a defined schema."""

    name: str = Field(description="Name of the dataset")
    description: str = Field(description="Detailed description of the dataset and its purpose")
    dataset_schema: DatasetSchema = Field(description="Schema defining the structure and fields of the dataset")
