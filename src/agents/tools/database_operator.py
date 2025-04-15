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
    """Key-value pairs representing the data for a single record. Must match the dataset's schema."""

    model_config = {
        "extra": "allow",  # Allow extra fields not defined in the model
    }


class ListDatasetsArgs(BaseModel):
    tool_call_id: Annotated[str, InjectedToolCallId]


class DatasetArgs(BaseModel):
    dataset_id: PydanticUUID = Field(description="The unique ID of the target dataset.")


class RecordArgs(DatasetArgs):
    record_id: PydanticUUID = Field(description="The unique ID of the target record within the dataset.")


class CreateDatasetArgs(BaseModel):
    name: str = Field(description="A descriptive name for the new dataset (1-100 chars).", min_length=1, max_length=100)
    description: str = Field(
        description="A detailed description of the dataset's purpose (1-500 chars).",
        min_length=1,
        max_length=500,
    )
    dataset_schema: DatasetSchema = Field(description="The schema defining the fields (columns) for this dataset.")


class UpdateDatasetArgs(DatasetArgs):
    name: str = Field(description="The new name for the dataset (1-100 chars).", min_length=1, max_length=100)
    description: str = Field(
        description="The new description for the dataset (1-500 chars).",
        min_length=1,
        max_length=500,
    )


class CreateRecordArgs(DatasetArgs):
    data: RecordData = Field(description="The data for the new record, matching the dataset's schema.")


class UpdateRecordArgs(RecordArgs):
    data: RecordData = Field(description="The new data for the record, matching the dataset's schema.")


class BatchCreateRecordsArgs(DatasetArgs):
    records: List[RecordData] = Field(description="A list of data objects, each representing a new record to create.", min_items=1)


class RecordUpdate(BaseModel):
    record_id: PydanticUUID = Field(description="ID of the record to update.")
    data: RecordData = Field(description="The new data for this specific record.")


class BatchUpdateRecordsArgs(DatasetArgs):
    records: List[RecordUpdate] = Field(description="A list of updates, each specifying a record ID and its new data.", min_items=1)


class BatchDeleteRecordsArgs(DatasetArgs):
    record_ids: List[PydanticUUID] = Field(description="A list of unique IDs for the records to be deleted.", min_items=1)


class QueryRecordsArgs(DatasetArgs):
    query: Optional[RecordQuery] = Field(default=None, description="Optional filters, sorting, or aggregation rules for the query.")
    ids_only: bool = Field(
        default=False,
        description="If True, return only record IDs (faster for finding records before updates/deletes). Ignored for aggregations.",
    )
    serialize_results: bool = Field(
        default=False,
        description="If True, may attach full results in Excel and return a partial list. Inform user if attachment exists. Use True ONLY for final user output. Returns tuple: (has_attachment, result_list).",
    )
    tool_call_id: Annotated[str, InjectedToolCallId]


class UpdateFieldArgs(DatasetArgs):
    field_name: str = Field(description="The current name of the field to modify.", min_length=1, max_length=100)
    field_update: SchemaField = Field(description="The new definition for the field (including name, type, constraints, options etc.).")


class DeleteFieldArgs(DatasetArgs):
    field_name: str = Field(description="The name of the field to remove from the schema.", min_length=1, max_length=100)


class AddFieldArgs(DatasetArgs):
    field: SchemaField = Field(description="The definition of the new field to add to the schema.")


class FindDatasetArgs(BaseModel):
    name: str = Field(description="An example name for the kind of dataset you're looking for.")
    description: str = Field(description="A description of the purpose of the dataset you're looking for.")
    dataset_schema: DatasetSchema = Field(description="An example schema representing the structure of the dataset you're looking for.")


class FindRecordArgs(BaseModel):
    dataset_id: PydanticUUID = Field(description="The ID of the dataset to search within.")
    record_data: RecordData = Field(description="Example data representing the record you're looking for (used for semantic search).")
    query: Optional[SimilarityQuery] = Field(
        default=None,
        description="Optional filters (on non-string fields like dates, numbers, booleans, select options) to apply *before* semantic search.",
    )


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


# region Tool Implementations
class ListDatasetsOperator(BaseInjectedToolCallIdDBOperator):
    MAX_TRUNCATED_DATASETS: int = 50  # Maximum number of datasets to show in truncated result

    name: str = "list_datasets"
    description: str = (
        """Lists all datasets. Returns (has_attachment, result_list). If results are numerous, `has_attachment` may be True (full results in attached Excel, partial list in `result_list` - inform user). Otherwise, `has_attachment` is False and `result_list` contains all datasets. This tool is for final user output only."""
    )
    args_schema: Type[BaseModel] = ListDatasetsArgs

    async def _arun(self, config: RunnableConfig, tool_call_id: Annotated[str, InjectedToolCallId], **kwargs) -> Tuple[List[Dict[str, Any]], bool]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = ListDatasetsArgs(**kwargs, tool_call_id=tool_call_id)
            datasets = await self.db.list_datasets(user_id)

            if not datasets:
                return False, []

            processed_result = [{"id": str(dataset.id), "name": dataset.name, "description": dataset.description} for dataset in datasets]

            # Always create an attachment if we have more datasets than the truncation limit
            if len(processed_result) > self.MAX_TRUNCATED_DATASETS:
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
    description: str = "Retrieves the full details (name, description, schema) of a specific dataset using its ID."
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
    description: str = "Retrieves only the schema (field definitions) of a specific dataset using its ID."
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
    description: str = "Creates a new dataset with a specified name, description, and schema. Returns the new dataset's ID."
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
    description: str = "Updates the name and/or description of an existing dataset."
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
    description: str = "Deletes an entire dataset and all its records. MUST ask for user confirmation before using."
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
    description: str = "Adds ONE new record to a dataset. For multiple records, use `batch_create_records`. Returns the new record's ID."
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
    description: str = "Updates the data of ONE specific record in a dataset. For multiple records, use `batch_update_records`."
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
    description: str = (
        "Deletes ONE specific record from a dataset. MUST ask for user confirmation before using. For multiple records, use `batch_delete_records`."
    )
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
    description: str = "Retrieves the full data of a specific record using its dataset ID and record ID."
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
    description: str = (
        """Queries records within a dataset using filters (exact matches, ranges on dates/numbers, etc.), sorting, or aggregation. Use this for precise filtering or when the user asks for exact matches. Returns (has_attachment, result_list). If `serialize_results=True` and results are numerous (and not aggregation), `has_attachment` may be True (full results in attached Excel, partial list in `result_list` - inform user). Otherwise, `has_attachment` is False and `result_list` contains all matching records/aggregation results. Use `ids_only=True` to get only IDs. Use `serialize_results=False` for internal processing."""
    )
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
    description: str = "Modifies an existing field's definition in a dataset's schema (e.g., rename, change type, add select options)."
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
    description: str = "Removes a field from a dataset's schema and its data from all records. MUST ask for user confirmation before using."
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
    description: str = "Adds a new field definition to a dataset's schema."
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
        "Adds MULTIPLE records to a dataset in a single operation. ALWAYS use this instead of `create_record` for bulk additions. Returns the new record IDs."
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
        "Updates MULTIPLE specific records in a dataset in a single operation. ALWAYS use this instead of `update_record` for bulk updates. Returns the IDs of updated records."
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
        "Deletes MULTIPLE specific records from a dataset in a single operation. MUST ask for user confirmation before using. ALWAYS use this instead of `delete_record` for bulk deletions. Returns the IDs of deleted records."
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


class FindDatasetOperator(BaseDBOperator):
    name: str = "find_dataset"
    description: str = (
        "Searches for existing datasets that are semantically similar to a provided example name, description, and schema. Use this to find potentially relevant datasets before creating a new one or searching records."
    )
    args_schema: Type[BaseModel] = FindDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        try:
            user_id = config.get("configurable", {}).get("user_id")
            args = FindDatasetArgs(**kwargs)
            dataset = Dataset(name=args.name, description=args.description, dataset_schema=args.dataset_schema, user_id=user_id)
            results = await self.db.search_similar_datasets(user_id, dataset)
            return [dataset.model_dump() for dataset in results]
        except Exception as e:
            logger.error(f"Error in FindDatasetOperator with args {kwargs}: {str(e)}", exc_info=True)
            raise


class FindRecord(BaseDBOperator):
    name: str = "find_record"
    description: str = (
        """DEFAULT search method. Performs semantic search (vector search) to find records similar to the provided `record_data` within a dataset. Ideal for finding records based on meaning or description in string fields when exact wording is unknown. Can optionally pre-filter using `query` on non-string fields (dates, numbers, booleans, select options) before the semantic search."""
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


class GetAllRecordsOperator(BaseDBOperator):
    name: str = "get_all_records"
    description: str = (
        "Retrieves ALL records from a specific dataset. Use `query_records` with filters or `find_record` if possible, especially for large datasets."
    )
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


# endregion
