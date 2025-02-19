"""Base models and utilities for the document store module."""

from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from pydantic import BaseModel, Field

from document_store.models.types import PydanticObjectId


class BaseDocument(BaseModel):
    """Base model for all document store models."""

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    user_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True, "from_attributes": True}
