from __future__ import annotations

from dataclasses import dataclass, field
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

from database_manager.schema_field import FieldType, SchemaField

if TYPE_CHECKING:
    from database_manager.collection_registry import CollectionRegistry


@dataclass
class CollectionDefinition:
    """
    Represents the definition of a collection including its schema and metadata.
    """

    name: str
    collection_registry: "CollectionRegistry"
    description: str
    schema: Dict[str, "SchemaField"]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    EMBEDDING_FIELD_NAME: str = "_embedding"

    @property
    def embedding(self) -> List[float]:
        """
        Get the embedding for this collection definition.
        """
        return self.generate_embedding()

    def get_content_for_embedding(self) -> str:
        """
        Prepare content used for generating an embedding.
        """
        content = {
            "name": self.name,
            "description": self.description,
            "schema": {
                name: {"field_type": field.field_type.value, "required": field.required, "default": field.default} for name, field in self.schema.items()
            },
        }
        return json.dumps(content)

    def generate_embedding(self) -> List[float]:
        """
        Generate an embedding vector for the collection definition.
        """
        content = self.get_content_for_embedding()
        return self.collection_registry.embeddings.embed_query(content)

    def to_dict(self) -> Dict[str, any]:
        """
        Convert the collection definition to a dictionary representation.
        """
        return {
            "name": self.name,
            "description": self.description,
            "schema": {name: field.to_dict() for name, field in self.schema.items()},
            "_created_at": self.created_at,
            "_updated_at": self.updated_at,
            self.EMBEDDING_FIELD_NAME: self.embedding,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any], collection_registry: "CollectionRegistry") -> CollectionDefinition:
        """
        Create a CollectionDefinition instance from a dictionary.
        """
        schema = {
            name: SchemaField(
                name=field_info["name"],
                description=field_info["description"],
                field_type=FieldType(field_info["field_type"]),
                required=field_info["required"],
                default=field_info["default"],
            )
            for name, field_info in data["schema"].items()
        }
        definition = cls(data["name"], collection_registry, data["description"], schema)
        definition.created_at = data["_created_at"]
        definition.updated_at = data["_updated_at"]
        return definition
