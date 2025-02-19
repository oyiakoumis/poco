"""Example usage of DatasetManager class demonstrating integration with LangChain workflow."""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from constants import DATABASE_CONNECTION_STRING
from document_store.dataset_manager import DatasetManager
from document_store.models.schema import DatasetSchema
from document_store.models.field import SchemaField
from document_store.models.types import FieldType
from document_store.models.query import (
    AggregationField,
    RecordQuery,
    FilterExpression,
    FilterCondition,
    ComparisonOperator,
)
from document_store.models.types import AggregationType


class ExampleDatasetManager(DatasetManager):
    """Example implementation using different collections."""

    DATABASE = "document_store_example"
    COLLECTION_DATASETS = "example_datasets"
    COLLECTION_RECORDS = "example_records"


async def main():
    """Run example operations."""
    # Initialize MongoDB client
    # Use the same connection string as the main application
    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    client.get_io_loop = asyncio.get_running_loop

    try:
        # Setup manager
        manager = await ExampleDatasetManager.setup(client)

        # Use same user_id format as main application
        user_id = "user_123"

        # 1. Dataset Operations
        print("\n=== Dataset Operations ===")

        # Create dataset with initial schema
        # Create a grocery list schema as used in main application
        schema = DatasetSchema(
            fields=[
                SchemaField(
                    field_name="item",
                    description="Name of the item",
                    type=FieldType.STRING,
                    required=True,
                ),
                SchemaField(
                    field_name="quantity",
                    description="Quantity of the item",
                    type=FieldType.INTEGER,
                    required=True,
                ),
                SchemaField(
                    field_name="unit", description="Unit of the quantity", type=FieldType.SELECT, required=True, options=["pieces", "kg", "g", "l", "ml"]
                ),
            ]
        )

        dataset_id = await manager.create_dataset(
            user_id=user_id,
            name="Grocery List",
            description="Example grocery list dataset",
            schema=schema,
        )
        print(f"Created dataset: {dataset_id}")

        # List datasets
        datasets = await manager.list_datasets(user_id)
        print(f"Found {len(datasets)} datasets")

        # Get specific dataset
        dataset = await manager.get_dataset(user_id, dataset_id)
        print(f"Retrieved dataset: {dataset.name}")

        # Update dataset
        await manager.update_dataset(
            user_id=user_id,
            dataset_id=dataset_id,
            name="My Grocery List",
            description="Updated grocery list dataset with categories",
        )
        print("Updated dataset metadata")

        # 2. Field Operations
        print("\n=== Field Operations ===")

        # Add category field as shown in main.py example
        new_field = SchemaField(
            field_name="category",
            description="Category of the item",
            type=FieldType.SELECT,
            required=False,
            options=["fruits", "vegetables", "dairy", "meat", "pantry", "beverages"],
        )
        await manager.add_field(user_id, dataset_id, new_field)
        print("Added category field")

        # Update unit field to add more options
        updated_field = SchemaField(
            field_name="unit", description="Unit of the quantity", type=FieldType.SELECT, required=True, options=["pieces", "kg", "g", "l", "ml", "pack", "box"]
        )
        await manager.update_field(user_id, dataset_id, "unit", updated_field)
        print("Updated unit field with more options")

        # 3. Record Operations
        print("\n=== Record Operations ===")

        # Create records
        record1_id = await manager.create_record(user_id=user_id, dataset_id=dataset_id, data={"item": "Milk", "quantity": 2, "unit": "l", "category": "dairy"})
        print(f"Created record 1: {record1_id}")

        record2_id = await manager.create_record(
            user_id=user_id, dataset_id=dataset_id, data={"item": "Apples", "quantity": 1, "unit": "kg", "category": "fruits"}
        )
        print(f"Created record 2: {record2_id}")

        # Get specific record
        record = await manager.get_record(user_id, dataset_id, record1_id)
        print(f"Retrieved record: {record.data}")

        # Update record
        await manager.update_record(
            user_id=user_id,
            dataset_id=dataset_id,
            record_id=record1_id,
            data={"item": "Milk", "quantity": 3, "unit": "l", "category": "dairy"},  # Updated quantity
        )
        print("Updated record 1")

        # Query records with filter
        filter_query = RecordQuery(filter=FilterExpression(field="category", condition=FilterCondition(operator=ComparisonOperator.EQUALS, value="dairy")))
        records = await manager.query_records(user_id=user_id, dataset_id=dataset_id, query=filter_query)
        print(f"Found {len(records)} dairy items")

        # 4. Aggregation
        print("\n=== Aggregation Operations ===")

        # Create query with aggregation
        agg_query = RecordQuery(
            group_by=["category"],
            aggregations=[
                AggregationField(field="quantity", operation=AggregationType.SUM, alias="total_quantity"),
                AggregationField(field="quantity", operation=AggregationType.COUNT, alias="total_items"),
            ],
        )

        results = await manager.query_records(user_id, dataset_id, agg_query)
        print("Aggregation results:", results)

        # Group by unit to see distribution of items by measurement unit
        unit_query = RecordQuery(
            group_by=["unit"],
            aggregations=[
                AggregationField(field="quantity", operation=AggregationType.SUM, alias="total_quantity"),
            ],
        )
        unit_results = await manager.query_records(user_id, dataset_id, unit_query)
        print("Items by unit:", unit_results)

        print("\n=== Delete Operations ===")
        # Delete category field (demonstrating field removal)
        await manager.delete_field(user_id, dataset_id, "category")
        print("Deleted category field")

        # Delete record
        await manager.delete_record(user_id, dataset_id, record2_id)
        print("Deleted record 2")

        # Delete dataset
        await manager.delete_dataset(user_id, dataset_id)
        print("Deleted dataset")

    finally:
        # Cleanup
        await client.drop_database(ExampleDatasetManager.DATABASE)
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
