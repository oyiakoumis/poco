from datetime import datetime
from typing import ClassVar, Dict, List, Optional

from langchain_core.embeddings import Embeddings
from langchain_core.tools import BaseTool
from langchain_openai.embeddings import OpenAIEmbeddings
from pydantic import BaseModel, Field

from database_connector import DatabaseConnector
from models.intent_model import IndexDefinition, IndexField, TableSchemaField
from utils import get_utc_now

# Constants
METADATA_COLLECTION = "table_metadata"
DEFAULT_TOP_K = 5
EMBEDDING_MODEL = "text-embedding-3-small"


# Common Models
class TableSchemaField(BaseModel):
    name: str
    type: str
    nullable: bool = True
    required: bool = False


class IndexDefinition(BaseModel):
    fields: List[IndexField]
    unique: bool = False


class BaseTableArgs(BaseModel):
    table_name: str = Field(description="The name of the table.")
    description: str = Field(description="A detailed description of the table's purpose and content.")


class CreateTableArgs(BaseTableArgs):
    table_schema: List[TableSchemaField] = Field(description="The schema of the table to create.")
    indexes: Optional[List[IndexDefinition]] = Field(default=None, description="A list of indexes to create for the table.")


class FindTableArgs(BaseTableArgs):
    table_name: Optional[str] = None
    table_schema: List[TableSchemaField] = Field(default=None, description="The expected schema of the table.")


# Base Table Operator
class BaseTableOperator(BaseTool):
    db: DatabaseConnector
    embeddings: Embeddings

    def __init__(self, db_connector: DatabaseConnector):
        super().__init__(db=db_connector, embeddings=OpenAIEmbeddings(model=EMBEDDING_MODEL))
        self._ensure_metadata_collection_exists()

    def _ensure_metadata_collection_exists(self) -> None:
        """Ensure the metadata collection exists with proper indexes."""
        if METADATA_COLLECTION not in self.db.list_collections():
            self.db.create_collection(METADATA_COLLECTION)
            self.db.create_index(METADATA_COLLECTION, [("table_name", "text")], unique=True)
            self.db.create_search_index(METADATA_COLLECTION, "embedding")

    def _generate_embedding(self, *text_parts: str) -> List[float]:
        """Generate embeddings for the concatenated text parts."""
        full_text = " ".join(filter(None, text_parts))
        return self.embeddings.embed_query(full_text)


class CreateTableOperator(BaseTableOperator):
    name: str = "create_table_operator"
    description: str = "Create a new table in the database."
    args_schema: ClassVar[BaseModel] = CreateTableArgs

    def _create_metadata_document(self, args: CreateTableArgs) -> Dict:
        """Create a metadata document with embeddings."""
        timestamp = get_utc_now()
        return {
            "table_name": args.table_name,
            "description": args.description,
            "table_schema": [schema_field.model_dump() for schema_field in args.table_schema],
            "created_at": timestamp,
            "updated_at": timestamp,
            "embedding": self._generate_embedding(args.table_name, args.description, str(args.table_schema)),
        }

    def _create_table_indexes(self, table_name: str, indexes: Optional[List[IndexDefinition]]) -> None:
        """Create indexes for the table."""
        if indexes:
            for index in indexes:
                self.db.create_index(table_name, index.fields, unique=index.unique)

    def _cleanup_on_failure(self, table_name: str, metadata_id: str = None, table_id: str = None) -> None:
        """Clean up resources if table creation fails."""
        if metadata_id:
            self.db.delete_document(METADATA_COLLECTION, metadata_id)
        if table_id:
            self.db.delete_document(table_name)

    def _run(self, **kwargs) -> Dict:
        """Create a new table and store its metadata."""
        args = CreateTableArgs(**kwargs)
        metadata_result = None
        table_result = None

        try:
            # Create metadata document
            metadata_doc = self._create_metadata_document(args)
            metadata_result = self.db.add_document(METADATA_COLLECTION, metadata_doc)

            # Create table and indexes
            table_result = self.db.create_collection(args.table_name)
            self._create_table_indexes(args.table_name, args.indexes)

            return {
                "status": "success",
                "message": f"Table {args.table_name} created successfully with metadata.",
                "result": table_result,
            }

        except Exception as e:
            if metadata_result:
                self._cleanup_on_failure(args.table_name, metadata_id=metadata_result.get("_id"), table_id=table_result.get("_id") if table_result else None)
            raise RuntimeError(f"Failed to create table: {str(e)}")


class FindTableOperator(BaseTableOperator):
    name: str = "find_table_operator"
    description: str = "Find a table in the metadata collection based on search criteria."
    args_schema: ClassVar[BaseModel] = FindTableArgs

    def _find_matching_tables(self, query_embedding: List[float], top_k: int = DEFAULT_TOP_K) -> List[Dict]:
        """Find the most relevant tables based on vector similarity."""
        return list(self.db.find_similar(METADATA_COLLECTION, "embedding", query_embedding, num_results=top_k))

    def _run(self, **kwargs) -> Dict:
        """Find similar tables based on input criteria."""
        args = FindTableArgs(**kwargs)

        query_embedding = self._generate_embedding(args.table_name, args.description, str(args.table_schema))

        matches = self._find_matching_tables(query_embedding)

        return {
            "status": "success",
            "message": "Matching tables found.",
            "results": matches,
        }


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
