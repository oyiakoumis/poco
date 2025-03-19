import asyncio
import io
import pandas as pd
from typing import Annotated, Any, Dict, List, Optional, Tuple, Type, Union

from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.prebuilt import InjectedState
from pydantic import BaseModel, Field

from agents.state import State
from database.document_store import DatasetManager
from database.document_store.models.dataset import Dataset
from database.document_store.models.field import SchemaField
from database.document_store.models.query import RecordQuery, SimilarityQuery
from database.document_store.models.schema import DatasetSchema
from models.base import PydanticUUID
from utils.logging import logger


class RecordData(BaseModel):
    """Record data model representing field values of a record."""

    model_config = {
        "extra": "allow",  # Allow extra fields not defined in the model
    }


class DatasetArgs(BaseModel):
    dataset_id: PydanticUUID = Field(description="Unique identifier for the dataset", json_schema_extra={"examples": ["507f1f77bcf86cd799439011"]})


class RecordArgs(DatasetArgs):
    record_id: PydanticUUID = Field(
        description="Unique identifier for the record within the dataset", json_schema_extra={"examples": ["507f1f77bcf86cd799439012"]}
    )


class CreateDatasetArgs(BaseModel):
    name: str = Field(description="Name of the dataset to be created", min_length=1, max_length=100, json_schema_extra={"examples": ["Customer Feedback"]})
    description: str = Field(
        description="Detailed description of the dataset's purpose and contents",
        min_length=1,
        max_length=500,
        example="Collection of customer feedback responses from Q1 2024",
    )
    dataset_schema: DatasetSchema = Field(
        description="List of field definitions that describe the schema of the dataset",
        example=[{"name": "feedback_text", "type": "string"}, {"name": "rating", "type": "integer", "min": 1, "max": 5}],
    )


class UpdateDatasetArgs(DatasetArgs):
    name: str = Field(description="Updated name for the dataset", min_length=1, max_length=100, json_schema_extra={"examples": ["Customer Feedback 2024"]})
    description: str = Field(
        description="Updated description for the dataset",
        min_length=1,
        max_length=500,
        example="Updated collection of customer feedback responses from Q1 2024",
    )


class CreateRecordArgs(DatasetArgs):
    data: RecordData = Field(
        description="Record data that matches the dataset's defined schema",
        example={"feedback_text": "Great product, but needs better documentation", "rating": 4},
    )


class UpdateRecordArgs(RecordArgs):
    data: RecordData = Field(
        description="Updated record data that matches the dataset's defined schema",
        example={"feedback_text": "Great product, documentation has improved", "rating": 5},
    )


class BatchCreateRecordsArgs(DatasetArgs):
    records: List[RecordData] = Field(
        description="List of record data objects that match the dataset's defined schema",
        example=[{"feedback_text": "Great product, but needs better documentation", "rating": 4}, {"feedback_text": "Works well, very intuitive", "rating": 5}],
        min_items=1,
    )


class RecordUpdate(BaseModel):
    record_id: PydanticUUID = Field(description="Unique identifier for the record to update", json_schema_extra={"examples": ["507f1f77bcf86cd799439012"]})
    data: RecordData = Field(
        description="Updated record data that matches the dataset's defined schema", example={"feedback_text": "Updated feedback", "rating": 5}
    )


class BatchUpdateRecordsArgs(DatasetArgs):
    records: List[RecordUpdate] = Field(
        description="List of record updates, each containing record_id and data",
        example=[
            {"record_id": "507f1f77bcf86cd799439012", "data": {"feedback_text": "Updated feedback", "rating": 5}},
            {"record_id": "507f1f77bcf86cd799439013", "data": {"feedback_text": "Another update", "rating": 4}},
        ],
        min_items=1,
    )


class BatchDeleteRecordsArgs(DatasetArgs):
    record_ids: List[PydanticUUID] = Field(
        description="List of record IDs to delete from the dataset", example=["507f1f77bcf86cd799439012", "507f1f77bcf86cd799439013"], min_items=1
    )


class QueryRecordsArgs(DatasetArgs):
    query: Optional[RecordQuery] = Field(
        default=None,
        description="Optional query parameters to filter, sort, or aggregate records",
        example={"filter": {"field": "rating", "condition": {"operator": "gte", "value": 4}}, "sort": {"created_at": False}},
    )
    ids_only: bool = Field(
        default=False,
        description="If True, returns only record IDs instead of full records (ignored for aggregation queries)",
    )
    truncate_results: bool = Field(
        default=False,
        description="If True, truncates results to 10 items and attaches full results as Excel file. Set to True when responding directly to user queries with a list of records.",
    )
    state: Annotated[State, InjectedState]


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
class BaseInjectedStateDBOperator(BaseDBOperator):
    def _run(self, config: RunnableConfig, state: Annotated[State, InjectedState], **kwargs):
        return asyncio.run(self._arun(config, state, **kwargs))


# List Dataset Operator
class ListDatasetsOperator(BaseDBOperator):
    name: str = "list_datasets"
    description: str = "List all datasets"

    async def _arun(self, config: RunnableConfig) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            datasets = await self.db.list_datasets(user_id)
            return [{"id": str(dataset.id), "name": dataset.name, "description": dataset.description} for dataset in datasets]
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


class QueryRecordsOperator(BaseInjectedStateDBOperator):
    name: str = "query_records"
    description: str = (
        "Query records with optional filtering, sorting, and aggregation. Supports both simple queries and aggregations. "
        "Use with ids_only=True when you only need record IDs (recommended for identifying records before update or delete operations to improve efficiency). "
        "Set truncate_results=True ONLY when responding directly to user queries - this will truncate results to 10 items and attach full results as Excel file. "
        "For intermediate processing steps, use truncate_results=False (default) to get complete results. "
        "Aggregation results are always returned in full regardless of truncate_results setting. "
        "Returns a tuple of (result, has_attachment) where has_attachment is a boolean indicating if an Excel file was attached to the state."
    )
    args_schema: Type[BaseModel] = QueryRecordsArgs

    async def _arun(self, config: RunnableConfig, state: Annotated[State, InjectedState], **kwargs) -> Tuple[Union[List[Dict[str, Any]], List[str]], bool]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = QueryRecordsArgs(**kwargs)
            result = await self.db.query_records(user_id, args.dataset_id, args.query, args.ids_only)

            if not result:
                return [], False

            # Handle record IDs - don't create Excel file for these
            if isinstance(result[0], str):  # Record IDs
                return result, False

            # Process different result types
            if isinstance(result[0], dict):  # Aggregation results
                # Always return full aggregation results
                processed_result = result
                return processed_result, False
            else:  # Record objects
                processed_result = [record.model_dump() for record in result]

            # Only truncate and create Excel attachment if truncate_results is True
            # and we're dealing with record objects (not aggregation results)
            if args.truncate_results and len(processed_result) > 10:
                # Create Excel file
                try:
                    # Get dataset name for the filename
                    dataset = await self.db.get_dataset(user_id, args.dataset_id)
                    dataset_name = dataset.name if dataset else str(args.dataset_id)

                    # Convert result to DataFrame
                    df = pd.DataFrame(processed_result)

                    # Create BytesIO object to store Excel file
                    excel_buffer = io.BytesIO()

                    # Write DataFrame to Excel file
                    df.to_excel(excel_buffer, index=False, engine="openpyxl")

                    # Get file size
                    file_size = excel_buffer.tell()

                    # Check if file size exceeds 16MB
                    if file_size > 16 * 1024 * 1024:  # 16MB in bytes
                        logger.error(f"Excel file size ({file_size} bytes) exceeds 16MB limit")
                        raise ValueError("Excel file size exceeds 16MB limit. Please refine your query to return fewer records.")

                    # Reset buffer position
                    excel_buffer.seek(0)

                    # Add file to state
                    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                    # Sanitize dataset name for filename and convert to lowercase
                    safe_dataset_name = "".join(c if c.isalnum() else "_" for c in dataset_name).lower()
                    filename = f"query_results_{safe_dataset_name}_{timestamp}.xlsx"

                    state["export_file_attachment"] = {
                        "filename": filename,
                        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "content": excel_buffer.getvalue(),
                        "size": file_size,
                    }

                    # Truncate result to 10 items
                    truncated_result = processed_result[:10]

                    # Return truncated result and flag indicating Excel file was added
                    return truncated_result, True

                except Exception as e:
                    logger.error(f"Error creating Excel file: {str(e)}", exc_info=True)
                    # If Excel creation fails, return the full result
                    return processed_result, False

            # Return the full result if truncate_results is False or result length is 10 or less
            return processed_result, False

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


class SearchSimilarDatasetsArgs(BaseModel):
    dataset: Dataset = Field(description="Dataset to find similar datasets to")


class SearchSimilarRecordsArgs(BaseModel):
    dataset_id: PydanticUUID = Field(description="Unique identifier for the dataset", json_schema_extra={"examples": ["507f1f77bcf86cd799439011"]})
    record_data: RecordData = Field(description="""Hypothetical record data to search in the dataset.""")
    query: Optional[SimilarityQuery] = Field(
        default=None,
        description="Optional query parameters to pre-filter records on non-string fields before semantic search",
        example={"filter": {"field": "status", "operator": "eq", "value": "active"}},
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


class SearchSimilarDatasetsOperator(BaseDBOperator):
    name: str = "search_similar_datasets"
    description: str = "Find similar datasets using vector search"
    args_schema: Type[BaseModel] = SearchSimilarDatasetsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = SearchSimilarDatasetsArgs(**kwargs)
            results = await self.db.search_similar_datasets(user_id, args.dataset)
            return [dataset.model_dump() for dataset in results]
        except Exception as e:
            logger.error(f"Error in SearchSimilarDatasetsOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class FindRecords(BaseDBOperator):
    name: str = "find_records"
    description: str = (
        "DEFAULT search method for finding records with string fields. Creates the hypothetical record that you are looking for using the dataset schema, and find candidates for this record using vector search. "
        "ALWAYS use this for searches involving string fields unless user explicitly requests exact matching. "
        "You can optionally provide a query to pre-filter records on non-string fields before semantic search. \n\n"
    )
    args_schema: Type[BaseModel] = SearchSimilarRecordsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = SearchSimilarRecordsArgs(**kwargs)
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
            logger.error(f"Error in FindRecordsByDescriptionOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise
