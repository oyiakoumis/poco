from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pymongo.operations import SearchIndexModel
from langchain_core.embeddings import Embeddings

from database_manager.collection_definition import CollectionDefinition
from database_manager.embedding_wrapper import EmbeddingWrapper

if TYPE_CHECKING:
    from database_manager.database import Database

logger = logging.getLogger(__name__)


class CollectionRegistry:
    """
    Registry to manage collection definitions and their metadata.
    """

    REGISTRY_COLLECTION_NAME = "_collection_registry"

    def __init__(self, database: "Database", embeddings: EmbeddingWrapper) -> None:
        """
        Initialize the collection registry.
        """
        self.database = database
        self.embeddings = embeddings
        self._mongo_collection = self.database._mongo_db[self.REGISTRY_COLLECTION_NAME]

    def init_registry(self) -> None:
        """
        Initialize the registry collection and ensure required indexes exist.
        """
        # TODO: Handle system collections filtering if necessary.
        collection_names = self.database._mongo_db.list_collection_names()
        if self.REGISTRY_COLLECTION_NAME not in collection_names:
            # Create unique index on collection name.
            self._mongo_collection.create_index("name", unique=True)

            # Create search index for embedding.
            self._create_vector_search_index()
            logger.info("Initialized collection registry with indexes.")

    def _create_vector_search_index(self) -> None:
        """Create the vector search index for embeddings."""
        search_index_model = SearchIndexModel(definition=self.embeddings.get_index_definition(), name=self.embeddings.config.index_name)
        self._mongo_collection.create_search_index(search_index_model)

    def register_collection(self, definition: CollectionDefinition) -> None:
        """
        Register a new collection definition.
        """
        self._mongo_collection.insert_one(definition.to_dict())
        logger.info("Registered collection definition for '%s'.", definition.name)

    def get_collection_definition(self, collection_name: str) -> Optional[CollectionDefinition]:
        """
        Retrieve the definition for a specific collection.
        """
        data = self._mongo_collection.find_one({"name": collection_name})
        if data:
            return CollectionDefinition.from_dict(data, self)
        logger.warning("Collection definition for '%s' not found.", collection_name)
        return None

    def list_collection_definitions(self) -> List[CollectionDefinition]:
        """
        List all registered collection definitions.
        """
        return [CollectionDefinition.from_dict(data, self) for data in self._mongo_collection.find()]

    def search_similar_collections(
        self, definition: CollectionDefinition, num_results: int = 5, min_score: float = 0.0, filter_dict: Optional[Dict[str, Any]] = None
    ) -> List[CollectionDefinition]:
        """
        Search for collections with similar embeddings.
        """
        pipeline = self.embeddings.get_search_pipeline(query_vector=definition.embedding, num_results=num_results, min_score=min_score, filter_dict=filter_dict)

        results = list(self._mongo_collection.aggregate(pipeline))
        return [CollectionDefinition.from_dict(data, self) for data in results]

    def update_collection_definition(self, definition: CollectionDefinition) -> None:
        """
        Update an existing collection definition.
        """
        definition.updated_at = datetime.now(timezone.utc)
        self._mongo_collection.update_one({"name": definition.name}, {"$set": definition.to_dict()})
        logger.info("Updated collection definition for '%s'.", definition.name)

    def unregister_collection(self, collection_name: str) -> None:
        """
        Unregister a collection from the registry.
        """
        self._mongo_collection.delete_one({"name": collection_name})
        logger.info("Unregistered collection definition for '%s'.", collection_name)
