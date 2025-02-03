from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List

from bson import ObjectId

if TYPE_CHECKING:
    from database_manager.collection import Collection

logger = logging.getLogger(__name__)


@dataclass
class Document:
    """
    Represents a document stored in a collection.
    """

    content: Dict[str, Any]
    collection: "Collection"
    id: ObjectId = field(default_factory=ObjectId)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    EMBEDDING_FIELD_NAME: str = "_embedding"

    @property
    def embedding(self) -> List[float]:
        """
        Get the embedding for the document's content.
        """
        return self.generate_embedding()

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the document to a dictionary representation.
        """
        return {
            "content": self.content,
            "_created_at": self.created_at,
            "_updated_at": self.updated_at,
            "_id": self.id,
            self.EMBEDDING_FIELD_NAME: self.embedding,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], collection: "Collection") -> Document:
        """
        Create a Document instance from a dictionary.
        """
        document: Document = cls(content=data["content"], collection=collection)
        document.id = data["_id"]
        document.created_at = data["_created_at"]
        document.updated_at = data["_updated_at"]
        return document

    def update(self) -> bool:
        """
        Update the document in the database.
        """
        # Validate before updating
        self.collection.validate_document(self.content)
        new_data = self.to_dict()
        new_updated_at = datetime.now(timezone.utc)
        new_data["_updated_at"] = new_updated_at

        result = self.collection._mongo_collection.update_one({"_id": self.id}, {"$set": new_data})

        if result.modified_count > 0:
            self.updated_at = new_updated_at
            logger.info("Document %s updated successfully", self.id)
            return True

        logger.warning("Document %s update failed", self.id)
        return False

    def delete(self) -> bool:
        """
        Delete the document from the database.
        """
        result = self.collection._mongo_collection.delete_one({"_id": self.id})
        if result.deleted_count > 0:
            logger.info("Document %s deleted successfully", self.id)
            return True

        logger.warning("Document %s deletion failed", self.id)
        return False

    def get_content_for_embedding(self) -> str:
        """
        Get the JSON string representation of the content for generating embeddings.
        """
        return json.dumps(self.content)

    def generate_embedding(self) -> List[float]:
        """
        Generate an embedding vector for the document's content.
        """
        content = self.get_content_for_embedding()
        return self.collection.embeddings.embed_query(content)
