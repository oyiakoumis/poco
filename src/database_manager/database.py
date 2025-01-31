from typing import Dict, Optional

from langchain_core.embeddings import Embeddings

from database_manager.collection import Collection
from database_manager.collection_registry import CollectionDefinition, CollectionRegistry
from database_manager.connection import Connection
from database_manager.schema_field import SchemaField


class Database:
    def __init__(self, name: str, connection: Connection, embeddings: Embeddings):
        self.name = name
        self.connection = connection
        self.embeddings = embeddings
        self.collections: Dict[str, Collection] = {}
        self._mongo_db = None
        self.registry = None

    def connect(self, restart=False) -> None:
        self.connection.connect()

        if restart:
            db = self.connection.client[self.name]
            for collection_name in db.list_collection_names():
                collection = db[collection_name]
                try:
                    collection.drop_search_index("*")
                except Exception:
                    pass
            self.connection.client.drop_database(self.name)

        self._mongo_db = self.connection.client[self.name]

        self.registry = CollectionRegistry(self, self.embeddings)
        self.registry.init_registry()
        self._load_existing_collections()

    def _load_existing_collections(self) -> None:
        """Load all collections from registry"""
        for definition in self.registry.list_collection_definitions():
            self.collections[definition.name] = Collection(definition.name, self, self.embeddings, definition.schema)

    def create_collection(self, name: str, schema: Dict[str, SchemaField], description: str) -> Collection:
        # Create definition first
        definition = CollectionDefinition(name, self.registry, description, schema)
        self.registry.register_collection(definition)

        # Create actual collection
        collection = Collection(name, self, self.embeddings, schema)
        collection.create_collection()
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
