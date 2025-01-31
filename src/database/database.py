from langchain_core.embeddings import Embeddings
from database.collection import Collection
from database.connection import Connection
from typing import Any, Dict, List, Optional
from database.collection_registry import CollectionDefinition, CollectionRegistry
from database.schema_field import SchemaField


class Database:
    def __init__(self, name: str, connection: Connection, embeddings: Embeddings):
        self.name = name
        self.connection = connection
        self.embeddings = embeddings
        self.collections: Dict[str, Collection] = {}
        self._mongo_db = None
        self.registry = None

    def connect(self) -> None:
        self.connection.connect()
        self._mongo_db = self.connection.client[self.name]
        self.registry = CollectionRegistry(self, self.embeddings)
        self.registry.init_registry()
        self._load_existing_collections()

    def _load_existing_collections(self) -> None:
        """Load all collections from registry"""
        for definition in self.registry.list_collection_definitions():
            self.collections[definition.name] = Collection(definition.name, self, definition.schema)

    def create_collection(self, name: str, schema: Dict[str, SchemaField], description: Optional[str] = None) -> Collection:
        if name in self.collections:
            raise ValueError(f"Collection {name} already exists")

        if not self._mongo_db:
            raise ValueError("Database not connected")

        # Create definition first
        definition = CollectionDefinition(name, description, schema)
        self.registry.register_collection(definition)

        # Create actual collection
        collection = Collection(name, self, schema)
        self.collections[name] = collection
        return collection

    def drop_collection(self, name: str) -> None:
        if name in self.collections:
            # Drop the actual collection
            self._mongo_db.drop_collection(name)
            # Remove from registry
            self.registry.unregister_collection(name)
            # Remove from local cache
            del self.collections[name]
