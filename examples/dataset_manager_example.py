"""Example usage of DatasetManager class demonstrating integration with LangChain workflow."""

import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from constants import DATABASE_CONNECTION_STRING
from database.document_store.dataset_manager import DatasetManager
from database.document_store.models.field import SchemaField
from database.document_store.models.query import (
    AggregationField,
    ComparisonOperator,
    FilterCondition,
    FilterExpression,
    LogicalOperator,
    RecordQuery,
)
from database.document_store.models.schema import DatasetSchema
from database.document_store.models.types import AggregationType, FieldType


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
        # Assert dataset_id is a valid string
        assert isinstance(dataset_id, str) and len(dataset_id) > 0, "Dataset ID should be a non-empty string"

        # List datasets
        datasets = await manager.list_datasets(user_id)
        print(f"Found {len(datasets)} datasets")
        # Assert datasets list contains at least one dataset
        assert len(datasets) > 0, "Expected at least one dataset"
        # Assert the created dataset is in the list
        assert any(str(ds.id) == dataset_id for ds in datasets), "Created dataset not found in datasets list"

        # Get specific dataset
        dataset = await manager.get_dataset(user_id, dataset_id)
        print(f"Retrieved dataset: {dataset.name}")
        # Assert retrieved dataset has the expected name and schema
        assert dataset.name == "Grocery List", f"Expected dataset name 'Grocery List', got '{dataset.name}'"
        assert len(dataset.dataset_schema.fields) == 4, f"Expected 4 fields in schema, got {len(dataset.dataset_schema.fields)}"
        assert any(field.field_name == "item" for field in dataset.dataset_schema.fields), "Field 'item' not found in schema"

        # Update dataset
        await manager.update_dataset(
            user_id=user_id,
            dataset_id=dataset_id,
            name="My Grocery List",
            description="Updated grocery list dataset with categories",
        )
        print("Updated dataset metadata")

        # Verify dataset was updated correctly
        updated_dataset = await manager.get_dataset(user_id, dataset_id)
        assert updated_dataset.name == "My Grocery List", f"Expected updated name 'My Grocery List', got '{updated_dataset.name}'"
        assert updated_dataset.description == "Updated grocery list dataset with categories", "Dataset description not updated correctly"

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

        # Verify field was added
        dataset_with_new_field = await manager.get_dataset(user_id, dataset_id)
        assert any(field.field_name == "category" for field in dataset_with_new_field.dataset_schema.fields), "Field 'category' not found in schema"
        category_field = next(field for field in dataset_with_new_field.dataset_schema.fields if field.field_name == "category")
        assert category_field.type == FieldType.SELECT, "Field 'category' has incorrect type"
        assert "dairy" in category_field.options, "Option 'dairy' not found in category field options"

        # Update unit field to add more options
        updated_field = SchemaField(
            field_name="unit", description="Unit of the quantity", type=FieldType.SELECT, required=True, options=["pieces", "kg", "g", "l", "ml", "pack", "box"]
        )
        await manager.update_field(user_id, dataset_id, "unit", updated_field)
        print("Updated unit field with more options")

        # Verify field was updated
        dataset_with_updated_field = await manager.get_dataset(user_id, dataset_id)
        unit_field = next(field for field in dataset_with_updated_field.dataset_schema.fields if field.field_name == "unit")
        assert "pack" in unit_field.options, "New option 'pack' not found in unit field options"
        assert "box" in unit_field.options, "New option 'box' not found in unit field options"

        # 3. Record Operations
        print("\n=== Record Operations ===")

        # Create records
        record1_id = await manager.create_record(user_id=user_id, dataset_id=dataset_id, data={"item": "Milk", "quantity": 2, "unit": "l", "category": "dairy"})
        print(f"Created record 1: {record1_id}")
        # Assert record ID is a valid string
        assert isinstance(record1_id, str) and len(record1_id) > 0, "Record ID should be a non-empty string"

        # Create record with expiry date
        record2_id = await manager.create_record(
            user_id=user_id,
            dataset_id=dataset_id,
            data={"item": "Yogurt", "quantity": 4, "unit": "pieces", "category": "dairy", "expiry_date": "2025-02-27"},  # Date in YYYY-MM-DD format
        )
        print(f"Created record 2: {record2_id}")
        assert isinstance(record2_id, str) and len(record2_id) > 0, "Record ID should be a non-empty string"
        assert record1_id != record2_id, "Record IDs should be unique"

        # Get specific record
        record = await manager.get_record(user_id, dataset_id, record1_id)
        print(f"Retrieved record: {record.data}")
        # Assert retrieved record has the expected data
        assert record.data["item"] == "Milk", f"Expected item 'Milk', got '{record.data['item']}'"
        assert record.data["quantity"] == 2, f"Expected quantity 2, got {record.data['quantity']}"
        assert record.data["unit"] == "l", f"Expected unit 'l', got '{record.data['unit']}'"
        assert record.data["category"] == "dairy", f"Expected category 'dairy', got '{record.data['category']}'"

        # Update record
        await manager.update_record(
            user_id=user_id,
            dataset_id=dataset_id,
            record_id=record1_id,
            data={"item": "Milk", "quantity": 3, "unit": "l", "category": "dairy"},  # Updated quantity
        )
        print("Updated record 1")

        # Verify record was updated
        updated_record = await manager.get_record(user_id, dataset_id, record1_id)
        assert updated_record.data["quantity"] == 3, f"Expected updated quantity 3, got {updated_record.data['quantity']}"

        # Get all records in the dataset
        all_records = await manager.get_all_records(user_id=user_id, dataset_id=dataset_id)
        print(f"Retrieved all {len(all_records)} records from dataset")
        # Assert all records were retrieved
        assert len(all_records) == 2, f"Expected 2 records, got {len(all_records)}"
        # Assert records contain expected data
        milk_record = next((r for r in all_records if r.data["item"] == "Milk"), None)
        yogurt_record = next((r for r in all_records if r.data["item"] == "Yogurt"), None)
        assert milk_record is not None, "Expected to find Milk record"
        assert yogurt_record is not None, "Expected to find Yogurt record"
        assert milk_record.data["quantity"] == 3, f"Expected Milk quantity 3, got {milk_record.data['quantity']}"
        assert yogurt_record.data["quantity"] == 4, f"Expected Yogurt quantity 4, got {yogurt_record.data['quantity']}"
        
        # Query records with simple filter
        simple_filter = FilterCondition(field="category", operator=ComparisonOperator.EQUALS, value="dairy")
        filter_query = RecordQuery(filter=simple_filter)
        records = await manager.query_records(user_id=user_id, dataset_id=dataset_id, query=filter_query)
        print(f"Found {len(records)} dairy items")
        # Assert query returned the expected number of records
        assert len(records) == 2, f"Expected 2 dairy items, got {len(records)}"
        # Assert all records have the expected category
        assert all(record.data["category"] == "dairy" for record in records), "Not all records have category 'dairy'"

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
        # Assert query returned the expected number of records
        assert len(and_records) == 2, f"Expected 1 dairy item with quantity > 2, got {len(and_records)}"
        # Assert all records match the criteria
        assert all(record.data["category"] == "dairy" and record.data["quantity"] > 2 for record in and_records), "Not all records match AND criteria"

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
        # Assert query returned the expected number of records
        assert len(or_records) == 2, f"Expected 2 items that are either dairy or have quantity > 3, got {len(or_records)}"
        # Assert all records match the criteria
        assert all(record.data["category"] == "dairy" or record.data["quantity"] > 3 for record in or_records), "Not all records match OR criteria"

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
        # Assert query returned the expected number of records
        assert len(nested_records) > 0, "Expected at least one record matching complex criteria"
        # Assert all records match the complex criteria
        for record in nested_records:
            category_match = record.data.get("category") == "dairy" or record.data.get("category") == "meat"
            quantity_match = record.data["quantity"] == 2 or record.data["quantity"] == 4
            assert category_match and quantity_match, f"Record does not match complex criteria: {record.data}"
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
        # Assert aggregation results have expected structure
        assert isinstance(results, list), "Aggregation results should be a list"
        assert len(results) > 0, "Expected at least one aggregation result"
        # Check if dairy category exists in results
        dairy_result = next((r for r in results if r.get("category") == "dairy"), None)
        assert dairy_result is not None, "Expected aggregation result for 'dairy' category"
        # Assert aggregated values are present
        assert "total_quantity" in dairy_result, "Expected 'total_quantity' in aggregation result"
        assert "total_items" in dairy_result, "Expected 'total_items' in aggregation result"
        # Verify the aggregated values
        assert dairy_result["total_items"] == 2, f"Expected 2 dairy items, got {dairy_result['total_items']}"
        assert dairy_result["total_quantity"] == 7, f"Expected total quantity 7 for dairy items, got {dairy_result['total_quantity']}"

        # Group by unit to see distribution of items by measurement unit
        unit_query = RecordQuery(
            group_by=["unit"],
            aggregations=[
                AggregationField(field="quantity", operation=AggregationType.SUM, alias="total_quantity"),
            ],
        )
        unit_results = await manager.query_records(user_id, dataset_id, unit_query)
        print("Items by unit:", unit_results)
        # Assert unit aggregation results have expected structure
        assert isinstance(unit_results, list), "Unit aggregation results should be a list"
        assert len(unit_results) > 0, "Expected at least one unit aggregation result"

        print("\n=== Delete Operations ===")
        # Delete category field (demonstrating field removal)
        await manager.delete_field(user_id, dataset_id, "category")
        print("Deleted category field")

        # Verify field was deleted
        dataset_after_field_deletion = await manager.get_dataset(user_id, dataset_id)
        assert not any(
            field.field_name == "category" for field in dataset_after_field_deletion.dataset_schema.fields
        ), "Field 'category' still exists in schema after deletion"

        # Delete record
        await manager.delete_record(user_id, dataset_id, record2_id)
        print("Deleted record 2")

        # Verify record was deleted
        try:
            deleted_record = await manager.get_record(user_id, dataset_id, record2_id)
            assert False, "Record should have been deleted but was retrieved"
        except Exception as e:
            # Expected exception when trying to get a deleted record
            assert "not found" in str(e).lower() or "does not exist" in str(e).lower(), f"Unexpected exception: {e}"

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
        # Assert similar datasets search returned expected results
        assert len(similar_datasets) > 0, "Expected at least one similar dataset"
        # The shopping list dataset should be similar to the grocery list
        shopping_dataset_found = False
        for ds in similar_datasets:
            if str(ds.id) == shopping_dataset_id:
                shopping_dataset_found = True
                break
        assert shopping_dataset_found, "Expected shopping list dataset to be similar to grocery list"
        for ds in similar_datasets:
            if str(ds.id) != dataset_id:  # Skip the reference dataset itself
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
