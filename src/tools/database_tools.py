from typing import ClassVar, Dict, List, Optional, Union

from pydantic import BaseModel, Field
from langchain_openai.embeddings import OpenAIEmbeddings
from langchain_core.tools import BaseTool
from langchain_core.embeddings import Embeddings

from database_connector import DatabaseConnector
from models.intent_model import IndexDefinition, TableSchemaField
from utils import get_utc_now


class CreateTableArgs(BaseModel):
    table_name: str = Field(description="The name of the table to create.")
    description: str = Field(description=("A detailed description of the table's purpose, content, and the type of data it is intended to store."))
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
    name: str = "create_table_operator"
    description: str = "Create a new table in the database."
    args_schema: ClassVar[BaseModel] = CreateTableArgs
    METADATA_COLLECTION: ClassVar[str] = "table_metadata"
    db: DatabaseConnector
    embeddings: Embeddings

    def __init__(self, db_connector: DatabaseConnector):
        super(CreateTableOperator, self).__init__(db=db_connector, embeddings=OpenAIEmbeddings(model="text-embedding-3-small"))

        self._ensure_metadata_collection_exists()

    def _ensure_metadata_collection_exists(self) -> None:
        """Ensure the metadata collection exists with proper indexes."""
        if self.METADATA_COLLECTION not in self.db.list_collections():
            self.db.create_collection(self.METADATA_COLLECTION)
            self.db.create_index(self.METADATA_COLLECTION, [("table_name", "text")], unique=True)

    def _generate_embedding(self, table_name: str, description: str, table_schema: List[TableSchemaField]) -> List[float]:
        """Generate embeddings for the table metadata."""
        full_text = f"{table_name} {description} {str(table_schema)}"
        return self.embeddings.embed_query(full_text)

    def _create_metadata_document(self, table_name: str, description: str, table_schema: List[TableSchemaField]) -> Dict:
        """Create a metadata document with embeddings."""
        timestamp = get_utc_now()
        return {
            "table_name": table_name,
            "description": description,
            "table_schema": [schema_field.model_dump() for schema_field in table_schema],
            "created_at": timestamp,
            "updated_at": timestamp,
            "embedding": self._generate_embedding(table_name, description, table_schema),
        }

    def _create_table_indexes(self, table: str, indexes: Optional[List[IndexDefinition]] = None) -> None:
        """Create indexes for the table based on the schema."""
        if indexes:
            for index in indexes:
                self.db.create_index(table, index["fields"], unique=index.get("unique", False))

    def _cleanup_on_failure(self, metadata_document_id: str, table_id: str = None) -> None:
        """Clean up resources if table creation fails."""
        # Remove metadata document if it exists
        self.db.delete_document(self.METADATA_COLLECTION, metadata_document_id)

        # Remove the table if it was created
        if table_id is not None:
            self.db.delete_document(table_id)

    def _run(self, table_name: str, description: str, table_schema: List[TableSchemaField], indexes: Optional[List[IndexDefinition]] = None) -> Dict:
        """Create a new table and store its metadata."""
        metadata_result = None
        table_result = None
        try:
            # Create and add the metadata document
            metadata_doc = self._create_metadata_document(table_name, description, table_schema)
            metadata_result = self.db.add_document(self.METADATA_COLLECTION, metadata_doc)

            # Create the table
            table_result = self.db.create_collection(table_name)
            self._create_table_indexes(table_name, indexes)

            return {
                "status": "success",
                "message": f"Table {table_name} created successfully with metadata.",
                "result": table_result,
            }

        except Exception as e:
            if metadata_result is not None:
                self._cleanup_on_failure(table_name, metadata_result["_id"], table_id=table_result["_id"] if table_result else None)
            raise RuntimeError(f"Failed to create table: {str(e)}")


class AddRecordsArgs(BaseModel):
    table_name: str = Field(description="The name of the table to add records to.")
    documents: List[Dict] = Field(description="The list of documents to add to the table.")


class AddRecordsOperator(BaseTool):
    name: str = "add_records_operator"
    description: str = "Add records to a table in the database."
    args_schema: ClassVar[BaseModel] = AddRecordsArgs
    db: DatabaseConnector

    def __init__(self, db_connector: DatabaseConnector):
        super(AddRecordsOperator, self).__init__(db=db_connector)

    def _run(self, table_name: str, documents: List[Dict]) -> Dict:
        """Add records to the specified table."""
        result = self.db.add_documents(table_name, documents)
        return {
            "status": "success",
            "message": f"Records added successfully to table {table_name}.",
            "result": result,
        }


class UpdateRecordsArgs(BaseModel):
    table_name: str = Field(description="The name of the table to update records in.")
    query: Dict = Field(description="The query to select records to update.")
    updates: Dict = Field(description="The updates to apply to the selected records.")


class UpdateRecordsOperator(BaseTool):
    name: str = "update_records_operator"
    description: str = "Update records in a table in the database."
    args_schema: ClassVar[BaseModel] = UpdateRecordsArgs
    db: DatabaseConnector

    def __init__(self, db_connector: DatabaseConnector):
        super(UpdateRecordsOperator, self).__init__(db=db_connector)

    def _run(self, table_name: str, query: Dict, updates: Dict) -> Dict:
        """Update records in the specified table."""
        result = self.db.update_documents(table_name, query, updates)
        return {
            "status": "success",
            "message": f"Records updated successfully in table {table_name}.",
            "result": result,
        }


class DeleteRecordsArgs(BaseModel):
    table_name: str = Field(description="The name of the table to delete records from.")
    query: Dict = Field(description="The query to select records to delete.")


class DeleteRecordsOperator(BaseTool):
    name: str = "delete_records_operator"
    description: str = "Delete records from a table in the database."
    args_schema: ClassVar[BaseModel] = DeleteRecordsArgs
    db: DatabaseConnector

    def __init__(self, db_connector: DatabaseConnector):
        super(DeleteRecordsOperator, self).__init__(db=db_connector)

    def _run(self, table_name: str, query: Dict) -> Dict:
        """Delete records from the specified table."""
        result = self.db.delete_documents(table_name, query)
        return {
            "status": "success",
            "message": f"Records deleted successfully from table {table_name}.",
            "result": result,
        }


class QueryRecordsArgs(BaseModel):
    table_name: str = Field(description="The name of the table to query records from.")
    query: Optional[Dict] = Field(default=None, description="The query to filter records.")


class QueryRecordsOperator(BaseTool):
    name: str = "query_records_operator"
    description: str = "Query records from a table in the database."
    args_schema: ClassVar[BaseModel] = QueryRecordsArgs
    db: DatabaseConnector

    def __init__(self, db_connector: DatabaseConnector):
        super(QueryRecordsOperator, self).__init__(db=db_connector)

    def _run(self, table_name: str, query: Optional[Dict] = None) -> Dict:
        """Query records in the specified table."""
        result = self.db.find_documents(table_name, query or {})
        return {
            "status": "success",
            "message": f"Records queried successfully from table {table_name}.",
            "result": result,
        }
