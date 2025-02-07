"""Example usage of the document store with MongoDB."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorClient

from constants import DATABASE_CONNECTION_STRING
from document_store.dataset_manager import DatasetManager
from document_store.exceptions import DatasetNotFoundError, InvalidRecordDataError
from document_store.types import SchemaField, FieldType


async def print_separator(title: str = "") -> None:
    """Print a separator line with optional title."""
    print("\n" + "=" * 80)
    if title:
        print(title)
        print("-" * 80)


async def print_dataset(dataset_id: str, manager: DatasetManager, user_id: str) -> None:
    """Print dataset details."""
    try:
        dataset = await manager.get_dataset(user_id, dataset_id)
        print(f"Dataset: {dataset.name}")
        print(f"Description: {dataset.description}")
        print("\nSchema:")
        for field in dataset.dataset_schema:
            print(f"  - {field.field_name} ({field.type.value})")
            print(f"    Description: {field.description}")
            print(f"    Required: {field.required}")
            if field.default is not None:
                print(f"    Default: {field.default}")
    except DatasetNotFoundError:
        print(f"Dataset {dataset_id} not found")


async def print_records(records: List[Dict[str, Any]]) -> None:
    """Print records in a tabular format."""
    if not records:
        print("No records found")
        return

    # Get all unique fields from records
    fields = set()
    for record in records:
        fields.update(record["data"].keys())
    fields = sorted(fields)

    # Print header
    header = "| " + " | ".join(fields) + " |"
    separator = "|" + "|".join("-" * (len(field) + 2) for field in fields) + "|"
    print(header)
    print(separator)

    # Print records
    for record in records:
        row = "| " + " | ".join(str(record["data"].get(field, "")) for field in fields) + " |"
        print(row)


async def main() -> None:
    """Run the demo."""
    # Connect to MongoDB
    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    client.get_io_loop = asyncio.get_running_loop

    # Initialize manager with proper setup
    manager = await DatasetManager.setup(client)
    user_id = "demo_user"

    try:
        # Clean up any existing dataset from previous runs
        existing_datasets = await manager.list_datasets(user_id)
        for dataset in existing_datasets:
            if dataset.name == "Employees":
                try:
                    await manager.delete_dataset(user_id, dataset.id)
                    print("Cleaned up existing dataset")
                except DatasetNotFoundError:
                    pass  # Dataset was already deleted

        await print_separator("Creating Dataset")
        # Create a dataset for employee records
        schema = [
            SchemaField(
                field_name="employee_id",
                description="Unique employee identifier",
                type=FieldType.STRING,
                required=True,
            ),
            SchemaField(
                field_name="name",
                description="Employee full name",
                type=FieldType.STRING,
                required=True,
            ),
            SchemaField(
                field_name="age",
                description="Employee age",
                type=FieldType.INTEGER,
                required=True,
            ),
            SchemaField(
                field_name="salary",
                description="Annual salary",
                type=FieldType.FLOAT,
                required=True,
            ),
            SchemaField(
                field_name="department",
                description="Department name",
                type=FieldType.STRING,
                required=True,
            ),
            SchemaField(
                field_name="notes",
                description="Additional notes",
                type=FieldType.STRING,
                required=False,
                default="",
            ),
        ]

        dataset_id = await manager.create_dataset(
            user_id=user_id,
            name="Employees",
            description="Employee records with salary information",
            schema=schema,
        )
        print(f"Created dataset with ID: {dataset_id}")
        await print_dataset(dataset_id, manager, user_id)

        await print_separator("Adding Records")
        # Add some employee records
        employees = [
            {
                "employee_id": "EMP001",
                "name": "John Doe",
                "age": 30,
                "salary": 75000.00,
                "department": "Engineering",
            },
            {
                "employee_id": "EMP002",
                "name": "Jane Smith",
                "age": 35,
                "salary": 85000.00,
                "department": "Engineering",
                "notes": "Team lead",
            },
            {
                "employee_id": "EMP003",
                "name": "Bob Wilson",
                "age": 40,
                "salary": 95000.00,
                "department": "Management",
            },
        ]

        for employee in employees:
            record_id = await manager.create_record(user_id, dataset_id, employee)
            print(f"Created record with ID: {record_id}")

        await print_separator("All Records")
        records = await manager.find_records(user_id, dataset_id)
        await print_records([record.model_dump(by_alias=True) for record in records])

        await print_separator("Engineering Department")
        # Find employees in Engineering department
        records = await manager.find_records(user_id, dataset_id, {"department": "Engineering"})
        await print_records([record.model_dump(by_alias=True) for record in records])

        await print_separator("Updating Record")
        # Update Jane's salary
        records = await manager.find_records(user_id, dataset_id, {"name": "Jane Smith"})
        if records:
            jane = records[0]
            jane_data = jane.data
            jane_data["salary"] = 90000.00
            await manager.update_record(user_id, dataset_id, jane.id, jane_data)
            print("Updated Jane's salary")

            # Verify the update
            updated = await manager.get_record(user_id, dataset_id, jane.id)
            print(f"Jane's new salary: ${updated.data['salary']:,.2f}")

        await print_separator("Invalid Data Demo")
        # Try to add a record with missing required field
        try:
            await manager.create_record(
                user_id,
                dataset_id,
                {
                    "name": "Invalid Record",
                    "age": 25,
                    # Missing required fields
                },
            )
        except InvalidRecordDataError as e:
            print(f"Failed to create invalid record: {e}")

        await print_separator("Cleanup")
        # Clean up by deleting the dataset
        await manager.delete_dataset(user_id, dataset_id)
        print("Deleted dataset and all its records")

        # Verify deletion
        try:
            await manager.get_dataset(user_id, dataset_id)
        except DatasetNotFoundError:
            print("Dataset successfully deleted")

    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
