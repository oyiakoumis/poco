from database.collection import Collection
from database.connection import Connection
from typing import Any, Dict, List, Optional
from database.schema_field import SchemaField


class Database:
    def __init__(self, name: str, connection: Connection):
        self.name = name
        self.connection = connection
        self.collections: Dict[str, Collection] = {}
        self._mongo_db = None

    def connect(self) -> None:
        self.connection.connect()
        self._mongo_db = self.connection.client[self.name]

    def disconnect(self) -> None:
        self.connection.disconnect()
        self._mongo_db = None

    def create_collection(self, name: str, schema: Dict[str, SchemaField] = None) -> Collection:
        if name in self.collections:
            raise ValueError(f"Collection {name} already exists")

        if not self._mongo_db:
            raise ValueError("Database not connected")

        collection = Collection(name, self, schema)
        self.collections[name] = collection
        return collection

    def get_collection(self, name: str) -> Optional[Collection]:
        return self.collections.get(name)

    def list_collections(self) -> List[str]:
        return list(self.collections.keys())
