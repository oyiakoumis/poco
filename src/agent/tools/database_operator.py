import asyncio
from typing import Any, ClassVar, Dict, List, Optional

from langchain_core.runnables.config import RunnableConfig
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from document_store import DatasetManager
from document_store.types import DatasetSchema
from document_store.types import PydanticObjectId, RecordData


class DatasetArgs(BaseModel):
    dataset_id: PydanticObjectId = Field(description="Unique identifier for the dataset", example="507f1f77bcf86cd799439011")


class RecordArgs(DatasetArgs):
    record_id: PydanticObjectId = Field(description="Unique identifier for the record within the dataset", example="507f1f77bcf86cd799439012")


class CreateDatasetArgs(BaseModel):
    name: str = Field(description="Name of the dataset to be created", min_length=1, max_length=100, example="Customer Feedback")
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
    name: str = Field(description="Updated name for the dataset", min_length=1, max_length=100, example="Customer Feedback 2024")
    description: str = Field(
        description="Updated description for the dataset",
        min_length=1,
        max_length=500,
        example="Updated collection of customer feedback responses from Q1 2024",
    )
    dataset_schema: DatasetSchema = Field(
        description="Updated list of field definitions for the dataset schema",
        example=[
            {"name": "feedback_text", "type": "string"},
            {"name": "rating", "type": "integer", "min": 1, "max": 5},
            {"name": "category", "type": "string", "enum": ["bug", "feature", "support"]},
        ],
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


class FindRecordsArgs(DatasetArgs):
    query: Optional[Dict] = Field(
        default=None, description="Optional query parameters to filter records in the dataset", example={"rating": {"$gte": 4}, "category": "feature"}
    )


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
        datasets = await self.db.list_datasets(user_id)
        return [dataset.model_dump() for dataset in datasets]


# Create Dataset Operator
class CreateDatasetOperator(BaseDBOperator):
    name: str = "create_dataset"
    description: str = "Create a new dataset"
    args_schema: ClassVar[BaseModel] = CreateDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, str]:
        user_id = config.get("configurable", {}).get("user_id")
        args = CreateDatasetArgs(**kwargs)
        result = await self.db.create_dataset(user_id, args.name, args.description, args.dataset_schema)
        return {"dataset_id": result}


# Update Dataset Operator
class UpdateDatasetOperator(BaseDBOperator):
    name: str = "update_dataset"
    description: str = "Update a dataset"
    args_schema: ClassVar[BaseModel] = UpdateDatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = UpdateDatasetArgs(**kwargs)
        await self.db.update_dataset(user_id, args.dataset_id, args.name, args.description, args.dataset_schema)


class DeleteDatasetOperator(BaseDBOperator):
    name: str = "delete_dataset"
    description: str = "Delete a dataset"
    args_schema: ClassVar[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = DatasetArgs(**kwargs)
        await self.db.delete_dataset(user_id, args.dataset_id)


class GetDatasetOperator(BaseDBOperator):
    name: str = "get_dataset"
    description: str = "Get a dataset"
    args_schema: ClassVar[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        user_id = config.get("configurable", {}).get("user_id")
        args = DatasetArgs(**kwargs)
        dataset = await self.db.get_dataset(user_id, args.dataset_id)
        return dataset.model_dump()


# Get All Records Operator
class GetAllRecordsOperator(BaseDBOperator):
    name: str = "get_all_records"
    description: str = "Get all records"
    args_schema: ClassVar[BaseModel] = DatasetArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        user_id = config.get("configurable", {}).get("user_id")
        args = DatasetArgs(**kwargs)
        records = await self.db.find_records(user_id, args.dataset_id)
        return [record.model_dump() for record in records]


class CreateRecordOperator(BaseDBOperator):
    name: str = "create_record"
    description: str = "Create a new record"
    args_schema: ClassVar[BaseModel] = CreateRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, str]:
        user_id = config.get("configurable", {}).get("user_id")
        args = CreateRecordArgs(**kwargs)
        result = await self.db.create_record(user_id, args.dataset_id, args.data)
        return {"record_id": result}


class UpdateRecordOperator(BaseDBOperator):
    name: str = "update_record"
    description: str = "Update a record"
    args_schema: ClassVar[BaseModel] = UpdateRecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = UpdateRecordArgs(**kwargs)
        await self.db.update_record(user_id, args.dataset_id, args.record_id, args.data)


class DeleteRecordOperator(BaseDBOperator):
    name: str = "delete_record"
    description: str = "Delete record"
    args_schema: ClassVar[BaseModel] = RecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> None:
        user_id = config.get("configurable", {}).get("user_id")
        args = RecordArgs(**kwargs)
        await self.db.delete_record(user_id, args.dataset_id, args.record_id)


class GetRecordOperator(BaseDBOperator):
    name: str = "get_record"
    description: str = "Get record"
    args_schema: ClassVar[BaseModel] = RecordArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> Dict[str, Any]:
        user_id = config.get("configurable", {}).get("user_id")
        args = RecordArgs(**kwargs)
        record = await self.db.get_record(user_id, args.dataset_id, args.record_id)
        return record.model_dump()


class FindRecordsOperator(BaseDBOperator):
    name: str = "find_records"
    description: str = "Find records"
    args_schema: ClassVar[BaseModel] = FindRecordsArgs

    async def _arun(self, config: RunnableConfig, **kwargs) -> List[Dict[str, Any]]:
        user_id = config.get("configurable", {}).get("user_id")
        args = FindRecordsArgs(**kwargs)
        result = await self.db.find_records(user_id, args.dataset_id, args.query)
        return [record.model_dump() for record in result]
