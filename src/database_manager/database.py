from typing import Dict, Optional

import logging
from pymongo.database import Database as MongoDatabase
from langchain_core.embeddings import Embeddings

from database_manager.collection import Collection
from database_manager.collection_registry import CollectionDefinition, CollectionRegistry
from database_manager.connection import Connection
from database_manager.schema_field import SchemaField

logger = logging.getLogger(__name__)


class Database:
    """
    Represents a database with collections and registry management.
    """

    def __init__(self, name: str, connection: Connection, embeddings: Embeddings) -> None:
        self.name = name
        self.connection = connection
        self.embeddings = embeddings
        self.collections: Dict[str, Collection] = {}
        self._mongo_db: Optional[MongoDatabase] = None
        self.registry: Optional[CollectionRegistry] = None

    def connect(self, restart: bool = False) -> None:
        """
        Connect to the database. If restart is True, drop existing collections and registry.
        """
        self.connection.connect()

        if restart:
            db = self.connection.client[self.name]
            for collection_name in db.list_collection_names():
                collection = db[collection_name]
                try:
                    collection.drop_search_index("*")
                except Exception as e:
                    logger.warning("Failed to drop search index on collection '%s': %s", collection_name, e)
            self.connection.client.drop_database(self.name)
            logger.info("Dropped existing database '%s'", self.name)

        self._mongo_db = self.connection.client[self.name]

        self.registry = CollectionRegistry(self, self.embeddings)
        self.registry.init_registry()
        self._load_existing_collections()
        logger.info("Connected to database '%s'", self.name)

    def _load_existing_collections(self) -> None:
        """
        Load all collections from the registry into the local cache.
        """
        for definition in self.registry.list_collection_definitions():
            self.collections[definition.name] = Collection(definition.name, self, self.embeddings, definition.schema)
        logger.info("Loaded %d collections from registry.", len(self.collections))

    def create_collection(self, name: str, schema: Dict[str, SchemaField], description: str) -> Collection:
        """
        Create a new collection and register its definition.
        """
        # Create definition first.
        definition = CollectionDefinition(name, self.registry, description, schema)
        self.registry.register_collection(definition)

        # Create actual collection instance and database collection.
        collection = Collection(name, self, self.embeddings, schema)
        collection.create_collection()
        self.collections[name] = collection
        logger.info("Created collection '%s'", name)
        return collection

    def drop_collection(self, name: str) -> None:
        """
        Drop a collection from the database and remove its registry entry.
        """
        if name in self.collections:
            # Drop the actual collection from the MongoDB database.
            self._mongo_db.drop_collection(name)
            # Remove from registry.
            self.registry.unregister_collection(name)
            # Remove from local cache.
            del self.collections[name]
            logger.info("Dropped collection '%s'", name)
