from datetime import datetime
from typing import Any, Dict

from typing import TYPE_CHECKING

from __future__ import annotations

if TYPE_CHECKING:
    from database.collection import Collection


class Document:
    def __init__(self, data: Dict[str, Any], collection: "Collection"):
        self.data = data
        self.collection = collection
        self.id = data.get("_id")
        self.created_at = data.get("created_at", datetime.now())
        self.updated_at = data.get("updated_at", datetime.now())

    def update(self, new_data: Dict[str, Any]) -> bool:
        # Validate before updating
        self.collection.validate_document(new_data)
        new_data["updated_at"] = datetime.now()

        result = self.collection._mongo_collection.update_one({"_id": self.id}, {"$set": new_data})

        if result.modified_count > 0:
            self.data.update(new_data)
            self.updated_at = new_data["updated_at"]
            return True
        return False

    def delete(self) -> bool:
        result = self.collection._mongo_collection.delete_one({"_id": self.id})
        return result.deleted_count > 0
