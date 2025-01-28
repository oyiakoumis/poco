from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from langchain_core.tools.base import BaseTool
from langchain_openai.embeddings import OpenAIEmbeddings

from database_connector import DatabaseConnector
from models.intent_model import IndexDefinition, TableSchemaField
from utils import get_utc_now


class TableMetadataModel(BaseModel):
    name: str = Field(description="The name of the table to create.")
    description: str = Field(description="Schema definition for the table")
    table_schema: List[TableSchemaField] = Field(
        description=(
            "The schema of the table to create, represented as a list of fields with their names, types, nullable status, and whether they are required."
        )
    )
    indexes: Optional[List[IndexDefinition]] = Field(
        default=None,
        description=("A list of indexes to create for the table. Each index specifies the fields to include, their sort order, and whether it is unique."),
    )


class CreateTableOperator(BaseTool):
    METADATA_COLLECTION = "tables_metadata"

    def __init__(self, db_connector: DatabaseConnector):
        super().__init__(args_schema=TableMetadataModel)

        self.db = db_connector
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self._ensure_metadata_collection_exists()

    def _ensure_metadata_collection_exists(self) -> None:
        """Ensure metadata collection exists with proper indexes."""
        if self.METADATA_COLLECTION not in self.db.list_collections():
            self.db.create_collection(self.METADATA_COLLECTION)
            self.db.create_index(self.METADATA_COLLECTION, [("collection_name", 1)], unique=True)

    def _generate_embedding(self, metadata: TableMetadataModel) -> list[float]:
        """Generate embeddings for the metadata."""
        full_text = f"{metadata.name} {metadata.description} {str(metadata.table_schema)}"
        return self.embeddings.embed_query(full_text)

    def _create_metadata_document(self, metadata: TableMetadataModel) -> Dict:
        """Create metadata document with embeddings."""
        return {
            "collection_name": metadata.name,
            "description": metadata.description,
            "schema": metadata.table_schema,
            "created_at": get_utc_now(),
            "updated_at": get_utc_now(),
            "embedding": self._generate_embedding(metadata),
        }

    def _create_collection_indexes(self, collection_name: str, indexes: List[Dict[str, str | bool]] = None) -> None:
        """Create indexes for the new collection based on schema."""
        if indexes:
            for index in indexes:
                self.db.create_index(collection_name, index["fields"], unique=index["unique"])

    def _run(self, name: str, description: str, table_schema: List[Dict[str, str]], indexes: List[Dict[str, str | bool]] = None) -> Dict:
        """Create a new table with its metadata."""
        metadata = TableMetadataModel(name=name, description=description, table_schema=table_schema)
        try:
            # Store metadata
            metadata_doc = self._create_metadata_document(metadata)
            self.db.add_document(self.METADATA_COLLECTION, metadata_doc)

            # Create table
            result = self.db.create_collection(name)
            self._create_collection_indexes(name, indexes)

            return {"status": "success", "message": f"Collection {name} created successfully with metadata", "result": result}

        except Exception as e:
            # Cleanup on failure
            self.db.delete_document(self.METADATA_COLLECTION, {"collection_name": name})
            raise Exception(f"Failed to create collection: {str(e)}")


class AddRecordsOperator(BaseTool):
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector

    def _run(self, collection_name: str, documents: list):
        return self.db_connector.add_documents(collection_name, documents)


class UpdateRecordsOperator(BaseTool):
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector

    def _run(self, collection_name: str, query: dict, updates: dict):
        return self.db_connector.update_documents(collection_name, query, updates)


class DeleteRecordsOperator(BaseTool):
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector

    def _run(self, collection_name: str, query: dict):
        return self.db_connector.delete_documents(collection_name, query)


class QueryRecordsOperator(BaseTool):
    def __init__(self, db_connector: DatabaseConnector):
        self.db_connector = db_connector

    def _run(self, collection_name: str, query: dict = {}):
        return self.db_connector.find_documents(collection_name, query)
