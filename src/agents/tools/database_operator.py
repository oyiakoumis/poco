import asyncio
from typing import Any, Dict, List, Optional, Type

from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from document_store import DatasetManager
from document_store.models.dataset import Dataset
from document_store.models.field import SchemaField
from document_store.models.query import RecordQuery
from document_store.models.record import RecordData
from document_store.models.schema import DatasetSchema
from models.base import PydanticUUID
from utils.logging import logger


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


class QueryRecordsArgs(DatasetArgs):
    query: Optional[RecordQuery] = Field(
        default=None,
        description="Optional query parameters to filter, sort, or aggregate records",
        example={"filter": {"field": "rating", "condition": {"operator": "gte", "value": 4}}, "sort": {"created_at": False}},
    )


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


# List Dataset Operator
class ListDatasetsOperator(BaseDBOperator):
    name: str = "list_datasets"
    description: str = "List all datasets"

    async def _arun(self, config: RunnableConfig) -> List[Dict[str, Any]]:
        user_id = config.get("configurable", {}).get("user_id")
        logger.debug(f"Listing datasets for user: {user_id}")
        datasets = await self.db.list_datasets(user_id)
        logger.debug(f"Found {len(datasets)} datasets")
        return [{"id": str(dataset.id), "name": dataset.name, "description": dataset.description} for dataset in datasets]


# Get Dataset Operator
class GetDatasetOperator(BaseDBOperator):
    name: str = "get_dataset"
    description: str = "Get a dataset by its ID to view schema details"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        user_id = config.get("configurable", {}).get("user_id")
        args = DatasetArgs(**kwargs)
        logger.info(f"Getting dataset {args.dataset_id} for user: {user_id}")
        dataset = await self.db.get_dataset(user_id, args.dataset_id)
        logger.info(f"Retrieved dataset: {dataset.name}")
        return dataset.model_dump()


# Create Dataset Operator
class CreateDatasetOperator(BaseDBOperator):
    name: str = "create_dataset"
    description: str = "Create a new dataset"
    args_schema: Type[BaseModel] = CreateDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, str]:
        user_id = config.get("configurable", {}).get("user_id")
        args = CreateDatasetArgs(**kwargs)
        logger.info(f"Creating dataset '{args.name}' for user: {user_id}")
        result = await self.db.create_dataset(user_id, args.name, args.description, args.dataset_schema)
        logger.info(f"Dataset created with ID: {result}")
        return {"dataset_id": result}


# Update Dataset Operator
class UpdateDatasetOperator(BaseDBOperator):
    name: str = "update_dataset"
    description: str = "Update a dataset"
    args_schema: Type[BaseModel] = UpdateDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = UpdateDatasetArgs(**kwargs)
        logger.info(f"Updating dataset {args.dataset_id} for user: {user_id}")
        await self.db.update_dataset(user_id, args.dataset_id, args.name, args.description)
        logger.info("Dataset updated successfully")


class DeleteDatasetOperator(BaseDBOperator):
    name: str = "delete_dataset"
    description: str = "Delete a dataset"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = DatasetArgs(**kwargs)
        logger.info(f"Deleting dataset {args.dataset_id} for user: {user_id}")
        await self.db.delete_dataset(user_id, args.dataset_id)
        logger.info("Dataset deleted successfully")


class CreateRecordOperator(BaseDBOperator):
    name: str = "create_record"
    description: str = f"Create a new record: {CreateRecordArgs.model_json_schema()}"
    args_schema: Type[BaseModel] = CreateRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, str]:
        user_id = config.get("configurable", {}).get("user_id")
        args = CreateRecordArgs(**kwargs)
        logger.info(f"Creating record in dataset {args.dataset_id} for user: {user_id}")
        result = await self.db.create_record(user_id, args.dataset_id, args.data)
        logger.info(f"Record created with ID: {result}")
        return {"record_id": result}


class UpdateRecordOperator(BaseDBOperator):
    name: str = "update_record"
    description: str = f"Update a record: {UpdateRecordArgs.model_json_schema()}"
    args_schema: Type[BaseModel] = UpdateRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = UpdateRecordArgs(**kwargs)
        logger.info(f"Updating record {args.record_id} in dataset {args.dataset_id}")
        await self.db.update_record(user_id, args.dataset_id, args.record_id, args.data)
        logger.info("Record updated successfully")


class DeleteRecordOperator(BaseDBOperator):
    name: str = "delete_record"
    description: str = "Delete record"
    args_schema: Type[BaseModel] = RecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = RecordArgs(**kwargs)
        logger.info(f"Deleting record {args.record_id} from dataset {args.dataset_id}")
        await self.db.delete_record(user_id, args.dataset_id, args.record_id)
        logger.info("Record deleted successfully")


class GetRecordOperator(BaseDBOperator):
    name: str = "get_record"
    description: str = "Get record"
    args_schema: Type[BaseModel] = RecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        user_id = config.get("configurable", {}).get("user_id")
        args = RecordArgs(**kwargs)
        record = await self.db.get_record(user_id, args.dataset_id, args.record_id)
        return record.model_dump()


class QueryRecordsOperator(BaseDBOperator):
    name: str = "query_records"
    description: str = "Query records with optional filtering, sorting, and aggregation. Supports both simple queries and aggregations."
    args_schema: Type[BaseModel] = QueryRecordsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        user_id = config.get("configurable", {}).get("user_id")
        args = QueryRecordsArgs(**kwargs)
        logger.info(f"Querying records in dataset {args.dataset_id}")
        result = await self.db.query_records(user_id, args.dataset_id, args.query)
        logger.info(f"Query returned {len(result) if result else 0} records")
        if not result:
            return []
        if isinstance(result[0], dict):
            return result
        return [record.model_dump() for record in result]


class UpdateFieldOperator(BaseDBOperator):
    name: str = "update_field"
    description: str = "Update a field in the dataset schema and convert existing records if needed"
    args_schema: Type[BaseModel] = UpdateFieldArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = UpdateFieldArgs(**kwargs)
        logger.info(f"Updating field {args.field_name} in dataset {args.dataset_id}")
        await self.db.update_field(user_id, args.dataset_id, args.field_name, args.field_update)
        logger.info("Field updated successfully")


class DeleteFieldOperator(BaseDBOperator):
    name: str = "delete_field"
    description: str = "Delete a field from the dataset schema"
    args_schema: Type[BaseModel] = DeleteFieldArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = DeleteFieldArgs(**kwargs)
        logger.info(f"Deleting field {args.field_name} from dataset {args.dataset_id}")
        await self.db.delete_field(user_id, args.dataset_id, args.field_name)
        logger.info("Field deleted successfully")


class AddFieldOperator(BaseDBOperator):
    name: str = "add_field"
    description: str = "Add a new field to the dataset schema"
    args_schema: Type[BaseModel] = AddFieldArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = AddFieldArgs(**kwargs)
        logger.info(f"Adding new field to dataset {args.dataset_id}")
        await self.db.add_field(user_id, args.dataset_id, args.field)
        logger.info("Field added successfully")


class SearchSimilarDatasetsArgs(BaseModel):
    dataset: Dataset = Field(description="Dataset to find similar datasets to")


class GetAllRecordsOperator(BaseDBOperator):
    name: str = "get_all_records"
    description: str = "Get all records in a dataset"
    args_schema: Type[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        user_id = config.get("configurable", {}).get("user_id")
        args = DatasetArgs(**kwargs)
        logger.info(f"Getting all records from dataset {args.dataset_id} for user: {user_id}")
        records = await self.db.get_all_records(user_id, args.dataset_id)
        logger.info(f"Retrieved {len(records)} records")
        return [record.model_dump() for record in records]


class SearchSimilarDatasetsOperator(BaseDBOperator):
    name: str = "search_similar_datasets"
    description: str = "Find similar datasets using vector search"
    args_schema: Type[BaseModel] = SearchSimilarDatasetsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        user_id = config.get("configurable", {}).get("user_id")
        args = SearchSimilarDatasetsArgs(**kwargs)
        results = await self.db.search_similar_datasets(user_id, args.dataset)
        return [dataset.model_dump() for dataset in results]
