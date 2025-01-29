from typing import List, Tuple, Union, Dict, Any
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.operations import SearchIndexModel
from bson import ObjectId

from utils import try_convert_to_int


class DatabaseConnector:
    def __init__(self, connection_string: str, database_name: str):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]

    def create_collection(self, collection_name: str) -> Collection:
        return self.db.create_collection(collection_name)

    def list_collections(self) -> List[str]:
        return self.db.list_collection_names()

    def create_index(self, collection_name: str, keys: List[Union[str, Tuple[str, str]]], unique: bool = False) -> str:
        collection = self.db[collection_name]
        # TODO: remove that if not useful
        # index_keys = [(key[0], try_convert_to_int(key[1])) for key in keys]
        return collection.create_index(keys, unique=unique)

    def create_vector_index(self, collection_name: str, vector_field: str, dimension: int = 1536):
        """Create a vector search index on the specified field."""
        search_index_model = SearchIndexModel(
            definition={"mappings": {"fields": [{"type": "vector", "numDimensions": dimension, "path": vector_field, "similarity": "cosine"}]}},
            name=f"{vector_field}_vector_index",
            type="vectorSearch",
        )
        self.db[collection_name].create_search_index(search_index_model)

    def find_similar(self, collection_name: str, vector_field: str, query_vector: list, num_results: int = 5, min_score: float = 0.0) -> list:
        pipeline = [
            {
                "$vectorSearch": {
                    "index": f"{vector_field}_vector_index",
                    "path": vector_field,
                    "queryVector": query_vector,
                    "numCandidates": num_results * 10,  # Adjust as needed
                    "limit": num_results,
                    "minScore": min_score,
                    "exact": False,  # Set to True for exact nearest neighbor search
                }
            },
            {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
        ]

        results = list(self.db[collection_name].aggregate(pipeline))
        return results

    def list_indexes(self, collection_name: str) -> List[Dict]:
        collection = self.db[collection_name]
        return list(collection.list_indexes())

    def drop_index(self, collection_name: str, index_name: str) -> None:
        collection = self.db[collection_name]
        collection.drop_index(index_name)

    def add_document(self, collection_name: str, document: Dict) -> ObjectId:
        collection = self.db[collection_name]
        return collection.insert_one(document).inserted_id

    def add_documents(self, collection_name: str, documents: List[Dict]) -> List[ObjectId]:
        collection = self.db[collection_name]
        return collection.insert_many(documents).inserted_ids

    def find_documents(self, collection_name: str, query: Dict = {}) -> List[Dict]:
        collection = self.db[collection_name]
        return list(collection.find(query))

    def update_document(self, collection_name: str, document_id: str, updates: Dict) -> Any:
        collection = self.db[collection_name]
        return collection.update_one({"_id": ObjectId(document_id)}, {"$set": updates})

    def update_documents(self, collection_name: str, query: Dict, updates: Dict) -> Any:
        collection = self.db[collection_name]
        return collection.update_many(query, {"$set": updates})

    def delete_document(self, collection_name: str, document_id: str) -> Any:
        collection = self.db[collection_name]
        return collection.delete_one({"_id": ObjectId(document_id)})

    def delete_documents(self, collection_name: str, query: Dict) -> Any:
        collection = self.db[collection_name]
        return collection.delete_many(query)

    def close(self) -> None:
        self.client.close()
