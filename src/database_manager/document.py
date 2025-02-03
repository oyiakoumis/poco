from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List

from bson import ObjectId

from database_manager.embedding_wrapper import Embeddable

if TYPE_CHECKING:
    from database_manager.collection import Collection

logger = logging.getLogger(__name__)


@dataclass
class Document(Embeddable):
    """
    Represents a document stored in a collection.
    """

    content: Dict[str, Any]
    collection: "Collection"
    id: ObjectId = field(default_factory=ObjectId)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def embedding(self) -> List[float]:
        """Get the embedding for the document's content"""
        return self.collection.embeddings.embed(self)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the document to a dictionary representation"""
        return {
            "content": self.content,
            "_created_at": self.created_at,
            "_updated_at": self.updated_at,
            "_id": self.id,
            self.collection.embeddings.config.field_name: self.embedding,
        }

    def get_content_for_embedding(self) -> str:
        """Get content for generating embeddings"""
        return json.dumps(self.content)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], collection: "Collection") -> Document:
        """Create a Document instance from a dictionary"""
        document = cls(content=data["content"], collection=collection)
        document.id = data["_id"]
        document.created_at = data["_created_at"]
        document.updated_at = data["_updated_at"]
        return document
