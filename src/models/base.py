from pydantic import BaseModel, Field


class BaseCollectionOperation(BaseModel):
    """Base model for collection operations."""

    target_collection: str = Field(min_length=1)
