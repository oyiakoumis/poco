from typing import ClassVar, Dict, List, Optional, Union
from pydantic import BaseModel, Field
from langchain_openai.embeddings import OpenAIEmbeddings
from database_connector import DatabaseConnector
from models.intent_model import IndexDefinition, TableSchemaField
from utils import get_utc_now


class CreateTableOperator:
    METADATA_COLLECTION: ClassVar[str] = "tables_metadata"

    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self._ensure_metadata_collection_exists()

    def _ensure_metadata_collection_exists(self) -> None:
        """Ensure the metadata collection exists with proper indexes."""
        if self.METADATA_COLLECTION not in self.db.list_collections():
            self.db.create_collection(self.METADATA_COLLECTION)
            self.db.create_index(self.METADATA_COLLECTION, [("collection_name", "text")], unique=True)

    def _generate_embedding(self, name: str, description: str, table_schema: List[Dict[str, str]]) -> List[float]:
        """Generate embeddings for the table metadata."""
        full_text = f"{name} {description} {str(table_schema)}"
        return self.embeddings.embed_query(full_text)

    def _create_metadata_document(self, name: str, description: str, table_schema: List[Dict[str, str]]) -> Dict:
        """Create a metadata document with embeddings."""
        return {
            "collection_name": name,
            "description": description,
            "schema": table_schema,
            "created_at": get_utc_now(),
            "updated_at": get_utc_now(),
            "embedding": self._generate_embedding(name, description, table_schema),
        }

    def _create_collection_indexes(self, collection_name: str, indexes: Optional[List[Dict[str, Union[str, bool]]]] = None) -> None:
        """Create indexes for the collection based on the schema."""
        if indexes:
            for index in indexes:
                self.db.create_index(collection_name, index["fields"], unique=index.get("unique", False))

    def __call__(self, name: str, description: str, table_schema: List[Dict[str, str]], indexes: Optional[List[Dict[str, Union[str, bool]]]] = None) -> Dict:
        """Create a new table and store its metadata."""
        metadata_doc = self._create_metadata_document(name, description, table_schema)

        try:
            self.db.add_document(self.METADATA_COLLECTION, metadata_doc)
            result = self.db.create_collection(name)
            self._create_collection_indexes(name, indexes)

            return {
                "status": "success",
                "message": f"Collection {name} created successfully with metadata.",
                "result": result,
            }

        except Exception as e:
            self.db.delete_document(self.METADATA_COLLECTION, {"collection_name": name})
            raise RuntimeError(f"Failed to create collection: {str(e)}")


class AddRecordsOperator:
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector

    def __call__(self, collection_name: str, documents: List[Dict]) -> Dict:
        """Add records to the specified collection."""
        result = self.db.add_documents(collection_name, documents)
        return {
            "status": "success",
            "message": f"Records added successfully to collection {collection_name}.",
            "result": result,
        }


class UpdateRecordsOperator:
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector

    def __call__(self, collection_name: str, query: Dict, updates: Dict) -> Dict:
        """Update records in the specified collection."""
        result = self.db.update_documents(collection_name, query, updates)
        return {
            "status": "success",
            "message": f"Records updated successfully in collection {collection_name}.",
            "result": result,
        }


class DeleteRecordsOperator:
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector

    def __call__(self, collection_name: str, query: Dict) -> Dict:
        """Delete records from the specified collection."""
        result = self.db.delete_documents(collection_name, query)
        return {
            "status": "success",
            "message": f"Records deleted successfully from collection {collection_name}.",
            "result": result,
        }


class QueryRecordsOperator:
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector

    def __call__(self, collection_name: str, query: Optional[Dict] = None) -> Dict:
        """Query records in the specified collection."""
        result = self.db.find_documents(collection_name, query or {})
        return {
            "status": "success",
            "message": f"Records queried successfully from collection {collection_name}.",
            "result": result,
        }
