from typing import TYPE_CHECKING, List
from __future__ import annotations
from pymongo.operations import SearchIndexModel

from database_manager.collection_definition import CollectionDefinition

if TYPE_CHECKING:
    from database_manager.database import Database


from typing import Optional
from datetime import datetime
from langchain_core.embeddings import Embeddings


class CollectionRegistry:
    REGISTRY_COLLECTION_NAME = "_collection_registry"
    EMBEDDING_DIMENSION = 1532  # TODO create a class for embedding
    EMBEDDING_INDEX_NAME = "embedding_index"

    def __init__(self, database: "Database", embeddings: Embeddings):
        self.database = database
        self.embeddings = embeddings
        self._mongo_collection = self.database._mongo_db[self.REGISTRY_COLLECTION_NAME]

    def init_registry(self):
        """Initialize the registry collection if it doesn't exist"""
        # TODO: Handle system collections filtering
        if self.REGISTRY_COLLECTION_NAME not in self.database._mongo_db.list_collection_names():
            self._mongo_collection.create_index("name", unique=True)

            search_index_definition = {
                "mappings": {
                    "dynamic": True,
                    "fields": {
                        CollectionDefinition.EMBEDDING_FIELD_NAME: {"type": "knnVector", "dimensions": self.EMBEDDING_DIMENSION, "similarity": "cosine"}
                    },
                }
            }
            search_index_model = SearchIndexModel(definition=search_index_definition, name=self.EMBEDDING_INDEX_NAME)
            self._mongo_collection.create_search_index(search_index_model)

    def register_collection(self, definition: CollectionDefinition) -> None:
        """Register a new collection definition"""
        definition.generate_embedding()
        self._mongo_collection.insert_one(definition.to_dict())

    def get_collection_definition(self, collection_name: str) -> Optional[CollectionDefinition]:
        """Get definition for a specific collection"""
        data = self._mongo_collection.find_one({"name": collection_name})
        return CollectionDefinition.from_dict(data) if data else None

    def list_collection_definitions(self) -> List[CollectionDefinition]:
        """Get definitions for all collections"""
        return [CollectionDefinition.from_dict(data) for data in self._mongo_collection.find()]

    def search_similar_collections(self, definition: CollectionDefinition, num_results: int = 5, min_score: float = 0.0) -> List[CollectionDefinition]:
        embedding = definition.generate_embedding()
        pipeline = [
            {
                "$vectorSearch": {
                    "index": self.EMBEDDING_INDEX_NAME,
                    "path": CollectionDefinition.EMBEDDING_FIELD_NAME,
                    "queryVector": embedding,
                    "numCandidates": num_results * 10,  # Adjust as needed
                    "limit": num_results,
                    "minScore": min_score,
                    "exact": False,  # Set to True for exact nearest neighbor search
                }
            },
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
            {"$project": {CollectionDefinition.EMBEDDING_FIELD_NAME: 0}},  # Excludes the vector field from the output
        ]

        results = list(self._mongo_collection.aggregate(pipeline))

        return [CollectionDefinition.from_dict(data) for data in results]

    def update_collection_definition(self, definition: CollectionDefinition) -> None:
        """Update definition for a collection"""
        definition.updated_at = datetime.now()
        definition.generate_embedding()
        self._mongo_collection.update_one({"name": definition.name}, {"$set": definition.to_dict()})

    def unregister_collection(self, collection_name: str) -> None:
        """Remove collection from registry"""
        self._mongo_collection.delete_one({"name": collection_name})
