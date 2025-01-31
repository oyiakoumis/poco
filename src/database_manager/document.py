from __future__ import annotations

from datetime import datetime
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from bson import ObjectId

if TYPE_CHECKING:
    from database_manager.collection import Collection


class Document:
    EMBEDDING_FIELD_NAME = "_embedding"

    def __init__(self, content: Dict[str, Any], collection: "Collection"):
        self.content = content
        self.collection = collection
        self.id: ObjectId = ObjectId()
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    @property
    def embedding(self) -> List[float]:
        return self.generate_embedding()

    def to_dict(self) -> str:
        return {
            "content": self.content,
            "_created_at": self.created_at,
            "_updated_at": self.updated_at,
            "_id": self.id,
            self.EMBEDDING_FIELD_NAME: self.embedding,
        }

    @classmethod
    def from_dict(cls, data: Dict, collection: "Collection") -> "Document":
        document: Document = cls(content=data["content"], collection=collection)
        document.id = data["_id"]
        document.created_at = data["_created_at"]
        document.updated_at = data["_updated_at"]

        return document

    def update(self) -> bool:
        # Validate before updating
        self.collection.validate_document(self.content)
        new_data = self.to_dict()
        new_data["_updated_at"] = datetime.now()

        result = self.collection._mongo_collection.update_one({"_id": self.id}, {"$set": new_data})

        if result.modified_count > 0:
            self.updated_at = new_data["_updated_at"]
            return True
        return False

    def delete(self) -> bool:
        result = self.collection._mongo_collection.delete_one({"_id": self.id})
        return result.deleted_count > 0

    def get_content_for_embedding(self) -> str:
        return json.dumps(self.content)

    def generate_embedding(self) -> None:
        content = self.get_content_for_embedding()
        return self.collection.embeddings.embed_query(content)
