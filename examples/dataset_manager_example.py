"""Example usage of DatasetManager class demonstrating integration with LangChain workflow."""

import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from constants import DATABASE_CONNECTION_STRING
from document_store.dataset_manager import DatasetManager
from document_store.models.field import SchemaField
from document_store.models.query import (
    AggregationField,
    ComparisonOperator,
    FilterCondition,
    FilterExpression,
    LogicalOperator,
    RecordQuery,
)
from document_store.models.schema import DatasetSchema
from document_store.models.types import AggregationType, FieldType


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
                SchemaField(
                    field_name="expiry_date",
                    description="Expiration date of the item",
                    type=FieldType.DATE,
                    required=False,
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

        # Create record with expiry date
        record2_id = await manager.create_record(
            user_id=user_id,
            dataset_id=dataset_id,
            data={"item": "Yogurt", "quantity": 4, "unit": "pieces", "category": "dairy", "expiry_date": "2025-02-27"},  # Date in YYYY-MM-DD format
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

        # Query records with simple filter
        simple_filter = FilterCondition(field="category", operator=ComparisonOperator.EQUALS, value="dairy")
        filter_query = RecordQuery(filter=simple_filter)
        records = await manager.query_records(user_id=user_id, dataset_id=dataset_id, query=filter_query)
        print(f"Found {len(records)} dairy items")

        # Query with AND condition: dairy items with quantity > 2
        and_filter = FilterExpression(
            operator=LogicalOperator.AND,
            expressions=[
                FilterCondition(field="category", operator=ComparisonOperator.EQUALS, value="dairy"),
                FilterCondition(field="quantity", operator=ComparisonOperator.GREATER_THAN, value=2),
            ],
        )
        and_query = RecordQuery(filter=and_filter)
        and_records = await manager.query_records(user_id=user_id, dataset_id=dataset_id, query=and_query)
        print(f"Found {len(and_records)} dairy items with quantity > 2")

        # Query with OR condition: dairy items or items with quantity > 3
        or_filter = FilterExpression(
            operator=LogicalOperator.OR,
            expressions=[
                FilterCondition(field="category", operator=ComparisonOperator.EQUALS, value="dairy"),
                FilterCondition(field="quantity", operator=ComparisonOperator.GREATER_THAN, value=3),
            ],
        )
        or_query = RecordQuery(filter=or_filter)
        or_records = await manager.query_records(user_id=user_id, dataset_id=dataset_id, query=or_query)
        print(f"Found {len(or_records)} items that are either dairy or have quantity > 3")

        # Complex nested query: (category=dairy OR category=meat) AND (quantity=2 OR quantity=4)
        nested_filter = FilterExpression(
            operator=LogicalOperator.AND,
            expressions=[
                FilterExpression(
                    operator=LogicalOperator.OR,
                    expressions=[
                        FilterCondition(field="category", operator=ComparisonOperator.EQUALS, value="dairy"),
                        FilterCondition(field="category", operator=ComparisonOperator.EQUALS, value="meat"),
                    ],
                ),
                FilterExpression(
                    operator=LogicalOperator.OR,
                    expressions=[
                        FilterCondition(field="quantity", operator=ComparisonOperator.EQUALS, value=2),
                        FilterCondition(field="quantity", operator=ComparisonOperator.EQUALS, value=4),
                    ],
                ),
            ],
        )
        nested_query = RecordQuery(filter=nested_filter)
        nested_records = await manager.query_records(user_id=user_id, dataset_id=dataset_id, query=nested_query)
        print("\nComplex nested query results:")
        print(f"Found {len(nested_records)} items that match:")
        print("- (category is dairy OR meat) AND")
        print("- (quantity is 2 OR 4)")
        for record in nested_records:
            print(f"- {record.data['item']}: category={record.data.get('category')}, quantity={record.data['quantity']}")

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

        # 5. Test Similar Datasets Search
        print("\n=== Similar Datasets Search ===")

        # Create additional datasets with different schemas for testing similarity
        # Recipe dataset
        recipe_schema = DatasetSchema(
            fields=[
                SchemaField(
                    field_name="name",
                    description="Name of the recipe",
                    type=FieldType.STRING,
                    required=True,
                ),
                SchemaField(
                    field_name="ingredients",
                    description="List of ingredients",
                    type=FieldType.STRING,
                    required=True,
                ),
                SchemaField(
                    field_name="servings",
                    description="Number of servings",
                    type=FieldType.INTEGER,
                    required=True,
                ),
                SchemaField(
                    field_name="cooking_time",
                    description="Cooking time in minutes",
                    type=FieldType.INTEGER,
                    required=True,
                ),
            ]
        )
        recipe_dataset_id = await manager.create_dataset(
            user_id=user_id,
            name="Recipe Book",
            description="Collection of favorite recipes",
            schema=recipe_schema,
        )
        print(f"Created recipe dataset: {recipe_dataset_id}")

        # Shopping list dataset (similar to grocery list)
        shopping_schema = DatasetSchema(
            fields=[
                SchemaField(
                    field_name="item",
                    description="Name of the item to buy",
                    type=FieldType.STRING,
                    required=True,
                ),
                SchemaField(
                    field_name="quantity",
                    description="Quantity needed",
                    type=FieldType.INTEGER,
                    required=True,
                ),
                SchemaField(
                    field_name="store",
                    description="Store to buy from",
                    type=FieldType.STRING,
                    required=True,
                ),
                SchemaField(
                    field_name="priority",
                    description="Shopping priority",
                    type=FieldType.SELECT,
                    required=False,
                    options=["low", "medium", "high"],
                ),
            ]
        )
        shopping_dataset_id = await manager.create_dataset(
            user_id=user_id,
            name="Shopping List",
            description="General shopping list for household items",
            schema=shopping_schema,
        )
        print(f"Created shopping dataset: {shopping_dataset_id}")

        # Test similar datasets search
        # Get the grocery list dataset to use as reference
        grocery_dataset = await manager.get_dataset(user_id, dataset_id)

        print("\nSearching for datasets similar to grocery list...")
        similar_datasets = await manager.search_similar_datasets(user_id=user_id, dataset=grocery_dataset, limit=5, min_score=0.7)

        print(f"\nFound {len(similar_datasets)} similar datasets:")
        for ds in similar_datasets:
            if ds.id != dataset_id:  # Skip the reference dataset itself
                print(f"- {ds.name}: {ds.description}")
                print("  Fields:")
                for field in ds.dataset_schema.fields:
                    print(f"    - {field.field_name} ({field.type.value})")
                print()

        # Cleanup
        print("\n=== Cleanup ===")
        await manager.delete_dataset(user_id, dataset_id)
        await manager.delete_dataset(user_id, recipe_dataset_id)
        await manager.delete_dataset(user_id, shopping_dataset_id)
        print("Deleted all datasets")

    finally:
        # Cleanup database
        await client.drop_database(ExampleDatasetManager.DATABASE)
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
