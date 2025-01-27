from typing import List, Union, Dict, Any
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from bson import ObjectId


class DatabaseConnector:
    def __init__(self, connection_string: str, database_name: str):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]

    def create_collection(self, collection_name: str) -> Collection:
        return self.db.create_collection(collection_name)

    def list_collections(self) -> List[str]:
        return self.db.list_collection_names()

    def create_index(self, collection_name: str, keys: List[Union[str, tuple]], unique: bool = False) -> str:
        collection = self.db[collection_name]
        index_keys = [(key, ASCENDING) if isinstance(key, str) else key for key in keys]
        return collection.create_index(index_keys, unique=unique)

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
