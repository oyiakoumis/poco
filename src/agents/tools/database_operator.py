import asyncio
from typing import Annotated, Any, Dict, List, Optional, Tuple, Type, Union

from langchain_core.messages import ToolMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import BaseTool, InjectedToolCallId
from langgraph.types import Command
from pydantic import BaseModel, Field

from database.document_store import DatasetManager
from database.document_store.models.dataset import Dataset
from database.document_store.models.field import SchemaField
from database.document_store.models.query import RecordQuery, SimilarityQuery
from database.document_store.models.schema import DatasetSchema
from models.base import PydanticUUID
from utils.logging import logger
from utils.xslx_serializer import serialize_to_xlsx


class RecordData(BaseModel):
    """Record data model representing field values of a record."""

    model_config = {
        "extra": "allow",  # Allow extra fields not defined in the model
    }


class ListDatasetsArgs(BaseModel):
    serialize_results: bool = Field(
        default=False,
        description="If True, tries to include an Excel file attached to the message with ALL datasets. Function returns a tuple of (has_attachment, result). This attached file is not directly accessible to the assistant. Therefore serialize_results=True should only be used to return a list of datasets directly to the user (not during intermediate steps).",
    )
    tool_call_id: Annotated[str, InjectedToolCallId]


class DatasetArgs(BaseModel):
    dataset_id: PydanticUUID = Field(description="Unique identifier for the dataset", examples=["507f1f77bcf86cd799439011"])


class RecordArgs(DatasetArgs):
    record_id: PydanticUUID = Field(description="Unique identifier for the record within the dataset", examples=["507f1f77bcf86cd799439012"])


class CreateDatasetArgs(BaseModel):
    name: str = Field(description="Name of the dataset to be created", min_length=1, max_length=100, examples=["Customer Feedback"])
    description: str = Field(
        description="Detailed description of the dataset's purpose and contents",
        min_length=1,
        max_length=500,
        examples="Collection of customer feedback responses from Q1 2024",
    )
    dataset_schema: DatasetSchema = Field(
        description="List of field definitions that describe the schema of the dataset",
        examples=[{"name": "feedback_text", "type": "string"}, {"name": "rating", "type": "integer", "min": 1, "max": 5}],
    )


class UpdateDatasetArgs(DatasetArgs):
    name: str = Field(description="Updated name for the dataset", min_length=1, max_length=100, examples=["Customer Feedback 2024"])
    description: str = Field(
        description="Updated description for the dataset",
        min_length=1,
        max_length=500,
        examples="Updated collection of customer feedback responses from Q1 2024",
    )


class CreateRecordArgs(DatasetArgs):
    data: RecordData = Field(
        description="Record data that matches the dataset's defined schema",
        examples={"feedback_text": "Great product, but needs better documentation", "rating": 4},
    )


class UpdateRecordArgs(RecordArgs):
    data: RecordData = Field(
        description="Updated record data that matches the dataset's defined schema",
        examples={"feedback_text": "Great product, documentation has improved", "rating": 5},
    )


class BatchCreateRecordsArgs(DatasetArgs):
    records: List[RecordData] = Field(
        description="List of record data objects that match the dataset's defined schema",
        examples=[
            {"feedback_text": "Great product, but needs better documentation", "rating": 4},
            {"feedback_text": "Works well, very intuitive", "rating": 5},
        ],
        min_items=1,
    )


class RecordUpdate(BaseModel):
    record_id: PydanticUUID = Field(description="Unique identifier for the record to update", examples=["507f1f77bcf86cd799439012"])
    data: RecordData = Field(
        description="Updated record data that matches the dataset's defined schema", examples={"feedback_text": "Updated feedback", "rating": 5}
    )


class BatchUpdateRecordsArgs(DatasetArgs):
    records: List[RecordUpdate] = Field(
        description="List of record updates, each containing record_id and data",
        examples=[
            {"record_id": "507f1f77bcf86cd799439012", "data": {"feedback_text": "Updated feedback", "rating": 5}},
            {"record_id": "507f1f77bcf86cd799439013", "data": {"feedback_text": "Another update", "rating": 4}},
        ],
        min_items=1,
    )


class BatchDeleteRecordsArgs(DatasetArgs):
    record_ids: List[PydanticUUID] = Field(
        description="List of record IDs to delete from the dataset", examples=["507f1f77bcf86cd799439012", "507f1f77bcf86cd799439013"], min_items=1
    )


class QueryRecordsArgs(DatasetArgs):
    query: Optional[RecordQuery] = Field(
        default=None,
        description="Optional query parameters to filter, sort, or aggregate records",
        examples={"filter": {"field": "rating", "condition": {"operator": "gte", "value": 4}}, "sort": {"created_at": False}},
    )
    ids_only: bool = Field(
        default=False,
        description="If True, returns only record IDs instead of full records (ignored for aggregation queries). Use this for identifying records before create, update or delete operations to improve efficiency.",
    )
    serialize_results: bool = Field(
        default=False,
        description="If True, tries to include an Excel file attached to the message with ALL records from the query. Function returns a tuple of (has_attachment, result). This attached file is not directly accessible to the assistant. Therefore serialize_results=True should only be used to return a list of records directly to the user (not during intermediate steps).",
    )
    tool_call_id: Annotated[str, InjectedToolCallId]


class UpdateFieldArgs(DatasetArgs):
    field_name: str = Field(description="Name of the field to update", min_length=1, max_length=100)
    field_update: SchemaField = Field(description="New field definition with updated properties")


class DeleteFieldArgs(DatasetArgs):
    field_name: str = Field(description="Name of the field to delete", min_length=1, max_length=100)


class AddFieldArgs(DatasetArgs):
    field: SchemaField = Field(description="New field definition to add to the dataset schema")


# Base Table Operator
class BaseDBOperator(BaseTool):
    db: DatasetManager

    def __init__(self, db: DatasetManager):
        super().__init__(db=db)

    def _run(self, config: RunnableConfig, **kwargs):
        return asyncio.run(self._arun(config, **kwargs))


# Base Table Operator with Injected State
class BaseInjectedToolCallIdDBOperator(BaseDBOperator):

    def _run(self, config: RunnableConfig, tool_call_id: Annotated[str, InjectedToolCallId], **kwargs):
        return asyncio.run(self._arun(config, tool_call_id, **kwargs))


# List Dataset Operator
class ListDatasetsOperator(BaseInjectedToolCallIdDBOperator):
    MAX_TRUNCATED_DATASETS: int = 50  # Maximum number of datasets to show in truncated result

    name: str = "list_datasets"
    description: str = """
This function is used to retrieve a list of all datasets. It returns a tuple: (has_attachment, result).

- When serialize_results=False (default):
  - has_attachment is False
  - result contains the complete list of datasets.

- When serialize_results=True:
  - If the Excel file was successfully attached:
    - has_attachment is True
    - result contains a partial list of datasets.
    - The assistant should clearly inform the user that:
      1. The displayed results are only a partial list, and
      2. The full list is available in the attached Excel file.
  - If the attachment fails:
    - has_attachment is False
    - result contains the full list of datasets.

For internal or intermediate processing, it's recommended to use serialize_results=False to ensure access to the complete dataset list.
"""
    args_schema: Type[BaseModel] = ListDatasetsArgs

    async def _arun(self, config: RunnableConfig, tool_call_id: Annotated[str, InjectedToolCallId], **kwargs) -> Tuple[List[Dict[str, Any]], bool]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = ListDatasetsArgs(**kwargs, tool_call_id=tool_call_id)
            datasets = await self.db.list_datasets(user_id)

            if not datasets:
                return False, []

            processed_result = [{"id": str(dataset.id), "name": dataset.name, "description": dataset.description} for dataset in datasets]

            # Only create an attachment if serialize_results is True
            # and we have more datasets than the truncation limit
            if args.serialize_results and len(processed_result) > self.MAX_TRUNCATED_DATASETS:
                # Create Excel file
                try:
                    # Prepare data for Excel
                    data_for_excel = [{"name": item["name"], "description": item["description"]} for item in processed_result]

                    # Serialize to Excel
                    excel_result = serialize_to_xlsx(data_for_excel, "Datasets")

                    # Truncate result to MAX_TRUNCATED_DATASETS items
                    truncated_result = processed_result[: self.MAX_TRUNCATED_DATASETS]

                    # Return truncated result and flag indicating Excel file was added
                    return Command(
                        update={
                            "messages": [ToolMessage(content=str((True, truncated_result)), tool_call_id=tool_call_id)],
                            "export_file_attachments": [excel_result],
                        }
                    )

                except Exception as e:
                    logger.error(f"Error creating Excel file: {str(e)}", exc_info=True)
                    # If Excel creation fails, return the full result
                    return False, processed_result

            # Return the full result if serialize_results is False or result length is MAX_TRUNCATED_DATASETS or less
            return False, processed_result

        except Exception as e:
            logger.error(f"Error in ListDatasetsOperator: {str(e)}", exc_info=True)
            raise


# Get Dataset Operator
class GetDatasetOperator(BaseDBOperator):
    name: str = "get_dataset"
    description: str = "Get a dataset by its ID to view all dataset details"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = DatasetArgs(**kwargs)
            dataset = await self.db.get_dataset(user_id, args.dataset_id)
            return dataset.model_dump()
        except Exception as e:
            logger.error(f"Error in GetDatasetOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


# Get Dataset Schema Operator
class GetDatasetSchemaOperator(BaseDBOperator):
    name: str = "get_dataset_schema"
    description: str = "Get only the schema of a dataset by its ID"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = DatasetArgs(**kwargs)
            schema = await self.db.get_dataset_schema(user_id, args.dataset_id)
            return schema.model_dump()
        except Exception as e:
            logger.error(f"Error in GetDatasetSchemaOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


# Create Dataset Operator
class CreateDatasetOperator(BaseDBOperator):
    name: str = "create_dataset"
    description: str = "Create a new dataset"
    args_schema: Type[BaseModel] = CreateDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, str]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = CreateDatasetArgs(**kwargs)
            result = await self.db.create_dataset(user_id, args.name, args.description, args.dataset_schema)
            return {"dataset_id": result}
        except Exception as e:
            logger.error(f"Error in CreateDatasetOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


# Update Dataset Operator
class UpdateDatasetOperator(BaseDBOperator):
    name: str = "update_dataset"
    description: str = "Update a dataset"
    args_schema: Type[BaseModel] = UpdateDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = UpdateDatasetArgs(**kwargs)
            await self.db.update_dataset(user_id, args.dataset_id, args.name, args.description)
        except Exception as e:
            logger.error(f"Error in UpdateDatasetOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class DeleteDatasetOperator(BaseDBOperator):
    name: str = "delete_dataset"
    description: str = "Delete a dataset"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = DatasetArgs(**kwargs)
            await self.db.delete_dataset(user_id, args.dataset_id)
        except Exception as e:
            logger.error(f"Error in DeleteDatasetOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class CreateRecordOperator(BaseDBOperator):
    name: str = "create_record"
    description: str = "Create a SINGLE new record. WARNING: Do NOT use this for creating multiple records - use batch_create_records instead."
    args_schema: Type[BaseModel] = CreateRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, str]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = CreateRecordArgs(**kwargs)
            # Convert RecordData to dict
            record_data = args.data.model_dump()
            result = await self.db.create_record(user_id, args.dataset_id, record_data)
            return {"record_id": result}
        except Exception as e:
            logger.error(f"Error in CreateRecordOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class UpdateRecordOperator(BaseDBOperator):
    name: str = "update_record"
    description: str = f"Update a SINGLE record. WARNING: Do NOT use this for updating multiple records - use batch_update_records instead."
    args_schema: Type[BaseModel] = UpdateRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = UpdateRecordArgs(**kwargs)
            # Convert RecordData to dict
            record_data = args.data.model_dump()
            await self.db.update_record(user_id, args.dataset_id, args.record_id, record_data)
        except Exception as e:
            logger.error(f"Error in UpdateRecordOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class DeleteRecordOperator(BaseDBOperator):
    name: str = "delete_record"
    description: str = "Delete a SINGLE record. WARNING: Do NOT use this for deleting multiple records - use batch_delete_records instead."
    args_schema: Type[BaseModel] = RecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = RecordArgs(**kwargs)
            await self.db.delete_record(user_id, args.dataset_id, args.record_id)
        except Exception as e:
            logger.error(f"Error in DeleteRecordOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class GetRecordOperator(BaseDBOperator):
    name: str = "get_record"
    description: str = "Get record"
    args_schema: Type[BaseModel] = RecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = RecordArgs(**kwargs)
            record = await self.db.get_record(user_id, args.dataset_id, args.record_id)
            return record.model_dump()
        except Exception as e:
            logger.error(f"Error in GetRecordOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class QueryRecordsOperator(BaseInjectedToolCallIdDBOperator):
    MAX_TRUNCATED_RECORDS: int = 50  # Maximum number of records to show in truncated result

    name: str = "query_records"
    description: str = """
This function is used to records with optional filtering, sorting, and aggregation. Supports both simple queries and aggregations.It returns a tuple: (has_attachment, result).

- Use `ids_only=True` when only record IDs are needed (recommended for identifying records before create, update or delete operations for better performance).

- When serialize_results=False (default):
  - has_attachment is False
  - result contains the complete list of records.

- When serialize_results=True:
  - If the Excel file was successfully attached:
    - has_attachment is True
    - result contains a partial list of records.
    - The assistant should clearly inform the user that:
      1. The displayed results are only a partial list, and
      2. The full list is available in the attached Excel file.
  - If the attachment fails:
    - has_attachment is False
    - result contains the full list of records.
  - Aggregation results are always returned in full without attachment regardless of serialize_results. 

For internal or intermediate processing, it's recommended to use serialize_results=False to ensure access to the complete dataset list.
"""
    args_schema: Type[BaseModel] = QueryRecordsArgs

    async def _arun(
        self, config: RunnableConfig, tool_call_id: Annotated[str, InjectedToolCallId], **kwargs
    ) -> Tuple[Union[List[Dict[str, Any]], List[str]], bool]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = QueryRecordsArgs(**kwargs, tool_call_id=tool_call_id)
            result = await self.db.query_records(user_id, args.dataset_id, args.query, args.ids_only)

            if not result:
                return (False, [])

            # Handle record IDs - don't create a file for these
            if isinstance(result[0], str):  # Record IDs
                return (False, result)

            # Aggregation results
            if isinstance(result[0], dict):
                return False, result
            else:  # Record objects
                processed_result = [record.model_dump() for record in result]

            # Only create an attachment if serialize_results is True
            # and we're dealing with record objects (not aggregation results)
            if args.serialize_results and len(processed_result) > self.MAX_TRUNCATED_RECORDS:
                # Create Excel file
                try:
                    # Extract data for Excel
                    data_for_excel = [record["data"] for record in processed_result]

                    # Get dataset name
                    dataset = await self.db.get_dataset(user_id, args.dataset_id)
                    dataset_name = dataset.name

                    # Serialize to Excel
                    excel_result = serialize_to_xlsx(data_for_excel, dataset_name)

                    # Truncate result to MAX_TRUNCATED_RECORDS items
                    truncated_result = processed_result[: self.MAX_TRUNCATED_RECORDS]

                    # Return truncated result and flag indicating Excel file was added
                    return Command(
                        update={
                            "messages": [ToolMessage(content=str((True, truncated_result)), tool_call_id=tool_call_id)],
                            "export_file_attachments": [excel_result],
                        }
                    )

                except Exception as e:
                    logger.error(f"Error creating Excel file: {str(e)}", exc_info=True)
                    # If Excel creation fails, return the full result
                    return False, processed_result

            # Return the full result if serialize_results is False or result length is MAX_TRUNCATED_RECORDS or less
            return False, processed_result

        except Exception as e:
            logger.error(f"Error in QueryRecordsOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class UpdateFieldOperator(BaseDBOperator):
    name: str = "update_field"
    description: str = "Update a field in the dataset schema and convert existing records if needed"
    args_schema: Type[BaseModel] = UpdateFieldArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = UpdateFieldArgs(**kwargs)
            await self.db.update_field(user_id, args.dataset_id, args.field_name, args.field_update)
        except Exception as e:
            logger.error(f"Error in UpdateFieldOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class DeleteFieldOperator(BaseDBOperator):
    name: str = "delete_field"
    description: str = "Delete a field from the dataset schema"
    args_schema: Type[BaseModel] = DeleteFieldArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = DeleteFieldArgs(**kwargs)
            await self.db.delete_field(user_id, args.dataset_id, args.field_name)
        except Exception as e:
            logger.error(f"Error in DeleteFieldOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class AddFieldOperator(BaseDBOperator):
    name: str = "add_field"
    description: str = "Add a new field to the dataset schema"
    args_schema: Type[BaseModel] = AddFieldArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = AddFieldArgs(**kwargs)
            await self.db.add_field(user_id, args.dataset_id, args.field)
        except Exception as e:
            logger.error(f"Error in AddFieldOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class BatchCreateRecordsOperator(BaseDBOperator):
    name: str = "batch_create_records"
    description: str = (
        "Create multiple records in a dataset at once. ALWAYS use this instead of create_record when you need to create multiple records in the same dataset."
    )
    args_schema: Type[BaseModel] = BatchCreateRecordsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, List[str]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = BatchCreateRecordsArgs(**kwargs)
            # Convert list of RecordData to list of dicts
            records_data = [record.model_dump() for record in args.records]
            record_ids = await self.db.batch_create_records(user_id, args.dataset_id, records_data)
            return {"record_ids": [str(record_id) for record_id in record_ids]}
        except Exception as e:
            logger.error(f"Error in BatchCreateRecordsOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class BatchUpdateRecordsOperator(BaseDBOperator):
    name: str = "batch_update_records"
    description: str = (
        "Update multiple records in a dataset at once. ALWAYS use this instead of update_record when you need to update multiple records in the same dataset."
    )
    args_schema: Type[BaseModel] = BatchUpdateRecordsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = BatchUpdateRecordsArgs(**kwargs)

            # Convert RecordUpdate objects to the dictionary format expected by batch_update_records
            record_updates = [{"record_id": record_update.record_id, "data": record_update.data.model_dump()} for record_update in args.records]

            updated_ids = await self.db.batch_update_records(user_id, args.dataset_id, record_updates)
            return {"updated_record_ids": [str(record_id) for record_id in updated_ids]}
        except Exception as e:
            logger.error(f"Error in BatchUpdateRecordsOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class BatchDeleteRecordsOperator(BaseDBOperator):
    name: str = "batch_delete_records"
    description: str = (
        "Delete multiple records from a dataset at once. ALWAYS use this instead of delete_record when you need to delete multiple records from the same dataset."
    )
    args_schema: Type[BaseModel] = BatchDeleteRecordsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = BatchDeleteRecordsArgs(**kwargs)

            deleted_ids = await self.db.batch_delete_records(user_id, args.dataset_id, args.record_ids)
            return {"deleted_record_ids": [str(record_id) for record_id in deleted_ids]}
        except Exception as e:
            logger.error(f"Error in BatchDeleteRecordsOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class FindDatasetArgs(BaseModel):
    dataset: Dataset = Field(description="Hypothetical dataset to search for in the database")


class FindRecordArgs(BaseModel):
    dataset_id: PydanticUUID = Field(description="Unique identifier for the dataset", examples=["507f1f77bcf86cd799439011"])
    record_data: RecordData = Field(description="Hypothetical record data to search in the dataset.")
    query: Optional[SimilarityQuery] = Field(
        default=None,
        description="Optional query parameters to pre-filter records on non-string fields before semantic search",
        examples={"filter": {"field": "status", "operator": "eq", "value": "active"}},
    )


class GetAllRecordsOperator(BaseDBOperator):
    name: str = "get_all_records"
    description: str = "Get all records in a dataset"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = DatasetArgs(**kwargs)
            records = await self.db.get_all_records(user_id, args.dataset_id)
            return [record.model_dump() for record in records]
        except Exception as e:
            logger.error(f"Error in GetAllRecordsOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class FindDatasetOperator(BaseDBOperator):
    name: str = "find_dataset"
    description: str = (
        "Find a dataset in the database. Creates the hypothetical dataset that you are looking for, and find candidates for this dataset using vector search."
    )
    args_schema: Type[BaseModel] = FindDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = FindDatasetArgs(**kwargs)
            results = await self.db.search_similar_datasets(user_id, args.dataset)
            return [dataset.model_dump() for dataset in results]
        except Exception as e:
            logger.error(f"Error in FindDatasetOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class FindRecord(BaseDBOperator):
    name: str = "find_record"
    description: str = (
        "DEFAULT search method for finding a record when we don't know the exact match for the string fields (Use Semantic Search). Creates the hypothetical record that you are looking for using the dataset schema, and find candidates for this record using vector search. "
        "ALWAYS use this for searches involving string fields unless user explicitly requests exact matching. "
        "You can optionally provide a query to pre-filter records on non-string fields before semantic search. \n\n"
    )
    args_schema: Type[BaseModel] = FindRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = FindRecordArgs(**kwargs)
            # Convert RecordData to dict
            record_data = args.record_data.model_dump()
            results = await self.db.search_similar_records(
                user_id=user_id,
                dataset_id=args.dataset_id,
                record_data=record_data,
                query=args.query,
            )
            return [record.model_dump() for record in results]
        except Exception as e:
            logger.error(f"Error in FindRecord with args {kwargs}: {str(e)}", exc_info=True)
            raise
