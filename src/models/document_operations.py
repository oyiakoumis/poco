from typing import List, Literal

from pydantic import Field

from models.base import BaseCollectionOperation
from models.fields import ConditionModel, DocumentModel


class AddDocumentsModel(BaseCollectionOperation):
    """Model for adding documents operation."""

    intent: Literal["add"]
    documents: List[List[DocumentModel]] = Field(min_items=1)


class UpdateDocumentsModel(BaseCollectionOperation):
    """Model for updating documents operation."""

    intent: Literal["update"]
    documents: List[DocumentModel] = Field(min_items=1)
    conditions: List[ConditionModel] = Field(default_factory=list)


class DeleteDocumentsModel(BaseCollectionOperation):
    """Model for deleting documents operation."""

    intent: Literal["delete"]
    conditions: List[ConditionModel] = Field(default_factory=list)
