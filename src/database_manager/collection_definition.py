import json
from typing import TYPE_CHECKING, Dict, List
from __future__ import annotations

from database_manager.schema_field import DataType, SchemaField

if TYPE_CHECKING:
    from database_manager.collection_registry import CollectionRegistry


from typing import Optional
from datetime import datetime


class CollectionDefinition:
    EMBEDDING_FIELD_NAME = "embedding"

    def __init__(self, name: str, collection_registry: "CollectionRegistry", description: Optional[str], schema: Dict[str, SchemaField]):
        self.name = name
        self.collection_registry = collection_registry
        self.description = description
        self.schema = schema
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.embedding: Optional[List[float]] = None

    def get_content_for_embedding(self) -> str:
        content = {
            "name": self.name,
            "description": self.description,
            "schema": {
                name: {"field_type": field.field_type.value, "required": field.required, "default": field.default} for name, field in self.schema.items()
            },
        }
        return json.dumps(content)

    def generate_embedding(self) -> None:
        content = self.get_content_for_embedding()
        self.embedding = self.collection_registry.embeddings.embed_query(content)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "schema": {
                name: {"field_type": field.field_type.value, "required": field.required, "default": field.default} for name, field in self.schema.items()
            },
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            self.EMBEDDING_FIELD_NAME: self.embedding,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "CollectionDefinition":
        schema = {
            name: SchemaField(
                name=field_info["name"],
                description=field_info["description"],
                field_type=DataType(field_info["field_type"]),
                required=field_info["required"],
                default=field_info["default"],
            )
            for name, field_info in data["schema"].items()
        }
        definition = cls(data["name"], data["description"], schema)
        definition.created_at = data["created_at"]
        definition.updated_at = data["updated_at"]
        definition.embedding = data.get(cls.EMBEDDING_FIELD_NAME)
        return definition
