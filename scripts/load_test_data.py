#!/usr/bin/env python
"""
Database initialization script for loading test data.

This script creates Tasks and Expenses datasets and populates them with
a large amount of sample data for testing the assistant's performance.
"""

import asyncio
import random
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorClient

from constants import DATABASE_CONNECTION_STRING
from database.document_store.dataset_manager import DatasetManager
from database.document_store.models.field import SchemaField
from database.document_store.models.schema import DatasetSchema
from database.document_store.models.types import FieldType


def generate_task_batch(count: int = 100, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Generate a large batch of realistic task data.

    Args:
        count: Number of tasks to generate
        start_date: Start date for task due dates (defaults to 30 days ago)
        end_date: End date for task due dates (defaults to 60 days from now)

    Returns:
        List of task data dictionaries
    """
    if not start_date:
        start_date = datetime.now() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now() + timedelta(days=60)

    tasks = []
    priorities = ["Low", "Medium", "High", "Critical"]
    tags = ["Work", "Personal", "Health", "Finance", "Education", "Home", "Shopping"]
    task_prefixes = [
        "Review",
        "Prepare",
        "Update",
        "Create",
        "Analyze",
        "Discuss",
        "Research",
        "Submit",
        "Organize",
        "Plan",
        "Attend",
        "Schedule",
        "Complete",
        "Follow up on",
    ]
    task_subjects = [
        "report",
        "presentation",
        "document",
        "meeting",
        "project",
        "budget",
        "proposal",
        "application",
        "email",
        "call",
        "interview",
        "assessment",
        "training",
        "workshop",
    ]

    for i in range(count):
        # Generate random due date between start and end dates
        days_range = (end_date - start_date).days
        random_days = random.randint(0, days_range)
        due_date = start_date + timedelta(days=random_days)

        # Add random time component for due_time
        due_time = datetime.combine(due_date.date(), time(hour=random.randint(8, 18), minute=random.choice([0, 15, 30, 45])))

        # Generate random task data
        task = {
            "title": f"{random.choice(task_prefixes)} {random.choice(task_subjects)} {i+1}",
            "description": f"This is a detailed description for task {i+1}",
            "due_date": due_date.strftime("%Y-%m-%d"),
            "due_time": due_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "completed": random.random() < 0.3,  # 30% chance of being completed
            "priority": random.choice(priorities),
            "estimated_hours": round(random.uniform(0.5, 8.0), 1),
            "tags": random.sample(tags, k=random.randint(1, 3)),  # 1-3 random tags
        }
        tasks.append(task)

    return tasks


def generate_expense_batch(count: int = 100, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Generate a large batch of realistic expense data.

    Args:
        count: Number of expenses to generate
        start_date: Start date for expenses (defaults to 90 days ago)
        end_date: End date for expenses (defaults to today)

    Returns:
        List of expense data dictionaries
    """
    if not start_date:
        start_date = datetime.now() - timedelta(days=90)
    if not end_date:
        end_date = datetime.now()

    expenses = []
    categories = ["Food", "Transportation", "Housing", "Utilities", "Entertainment", "Healthcare", "Education", "Shopping", "Travel", "Other"]
    payment_methods = ["Cash", "Credit Card", "Debit Card", "Bank Transfer", "Mobile Payment", "Check"]
    expense_descriptions = [
        "Grocery shopping",
        "Restaurant meal",
        "Coffee shop",
        "Gas station",
        "Uber ride",
        "Bus ticket",
        "Rent payment",
        "Electricity bill",
        "Water bill",
        "Internet service",
        "Phone bill",
        "Movie tickets",
        "Concert tickets",
        "Doctor visit",
        "Prescription medication",
        "Books",
        "Online course",
        "Clothing purchase",
        "Electronics",
        "Home supplies",
        "Hotel stay",
        "Flight tickets",
        "Car maintenance",
        "Gym membership",
        "Subscription service",
    ]

    for i in range(count):
        # Generate random date between start and end dates
        days_range = (end_date - start_date).days
        random_days = random.randint(0, days_range)
        expense_date = start_date + timedelta(days=random_days)

        # Generate random amount based on category
        category = random.choice(categories)
        if category in ["Housing", "Travel"]:
            amount = round(random.uniform(100, 2000), 2)
        elif category in ["Healthcare", "Education"]:
            amount = round(random.uniform(50, 500), 2)
        else:
            amount = round(random.uniform(5, 150), 2)

        # Generate random expense data
        expense = {
            "description": random.choice(expense_descriptions),
            "amount": amount,
            "date": expense_date.strftime("%Y-%m-%d"),
            "category": category,
            "payment_method": random.choice(payment_methods),
            "recurring": random.random() < 0.2,  # 20% chance of being recurring
            "tax_deductible": random.random() < 0.15,  # 15% chance of being tax deductible
            "receipt_photo": f"receipt_{i+1}.jpg" if random.random() < 0.3 else None,  # 30% chance of having receipt
        }
        expenses.append(expense)

    return expenses


async def create_tasks_dataset(db: DatasetManager, user_id: str) -> str:
    """Create the Tasks dataset with appropriate schema.

    Args:
        db: Database manager instance
        user_id: User ID to associate with the dataset

    Returns:
        Dataset ID
    """
    # Check if dataset already exists
    datasets = await db.list_datasets(user_id)
    for dataset in datasets:
        if dataset.name == "Tasks":
            print(f"Tasks dataset already exists with ID: {dataset.id}")
            return str(dataset.id)

    # Create schema
    tasks_schema = DatasetSchema(
        fields=[
            SchemaField(field_name="title", description="Task title", type=FieldType.STRING, required=True),
            SchemaField(field_name="description", description="Detailed task description", type=FieldType.STRING),
            SchemaField(field_name="due_date", description="Due date for the task", type=FieldType.DATE),
            SchemaField(field_name="due_time", description="Due time for the task", type=FieldType.DATETIME),
            SchemaField(field_name="completed", description="Whether the task is completed", type=FieldType.BOOLEAN, default=False),
            SchemaField(
                field_name="priority", description="Task priority level", type=FieldType.SELECT, options=["Low", "Medium", "High", "Critical"], default="Medium"
            ),
            SchemaField(field_name="estimated_hours", description="Estimated hours to complete", type=FieldType.FLOAT),
            SchemaField(
                field_name="tags",
                description="Task tags or categories",
                type=FieldType.MULTI_SELECT,
                options=["Work", "Personal", "Health", "Finance", "Education", "Home", "Shopping"],
            ),
        ]
    )

    # Create dataset
    dataset_id = await db.create_dataset(user_id=user_id, name="Tasks", description="Collection of tasks and to-do items", schema=tasks_schema)

    print(f"Created Tasks dataset with ID: {dataset_id}")
    return str(dataset_id)


async def create_expenses_dataset(db: DatasetManager, user_id: str) -> str:
    """Create the Expenses dataset with appropriate schema.

    Args:
        db: Database manager instance
        user_id: User ID to associate with the dataset

    Returns:
        Dataset ID
    """
    # Check if dataset already exists
    datasets = await db.list_datasets(user_id)
    for dataset in datasets:
        if dataset.name == "Expenses":
            print(f"Expenses dataset already exists with ID: {dataset.id}")
            return str(dataset.id)

    # Create schema
    expenses_schema = DatasetSchema(
        fields=[
            SchemaField(field_name="description", description="Expense description", type=FieldType.STRING, required=True),
            SchemaField(field_name="amount", description="Expense amount", type=FieldType.FLOAT, required=True),
            SchemaField(field_name="date", description="Date of expense", type=FieldType.DATE, required=True),
            SchemaField(
                field_name="category",
                description="Expense category",
                type=FieldType.SELECT,
                options=["Food", "Transportation", "Housing", "Utilities", "Entertainment", "Healthcare", "Education", "Shopping", "Travel", "Other"],
            ),
            SchemaField(
                field_name="payment_method",
                description="Method of payment",
                type=FieldType.SELECT,
                options=["Cash", "Credit Card", "Debit Card", "Bank Transfer", "Mobile Payment", "Check"],
            ),
            SchemaField(field_name="recurring", description="Whether this is a recurring expense", type=FieldType.BOOLEAN, default=False),
            SchemaField(field_name="tax_deductible", description="Whether the expense is tax deductible", type=FieldType.BOOLEAN, default=False),
            SchemaField(field_name="receipt_photo", description="Link to receipt photo", type=FieldType.STRING),
        ]
    )

    # Create dataset
    dataset_id = await db.create_dataset(user_id=user_id, name="Expenses", description="Collection of personal and business expenses", schema=expenses_schema)

    print(f"Created Expenses dataset with ID: {dataset_id}")
    return str(dataset_id)


async def load_task_batch(db: DatasetManager, user_id: str, dataset_id: str, tasks: List[Dict[str, Any]]) -> None:
    """Load a batch of tasks into the database.

    Args:
        db: Database manager instance
        user_id: User ID to associate with the records
        dataset_id: Dataset ID to add records to
        tasks: List of task data dictionaries
    """
    batch_size = 50  # Process in smaller batches to avoid overwhelming the database
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        await db.batch_create_records(user_id, dataset_id, batch)
        print(f"Loaded tasks {i+1} to {min(i+batch_size, len(tasks))}")


async def load_expense_batch(db: DatasetManager, user_id: str, dataset_id: str, expenses: List[Dict[str, Any]]) -> None:
    """Load a batch of expenses into the database.

    Args:
        db: Database manager instance
        user_id: User ID to associate with the records
        dataset_id: Dataset ID to add records to
        expenses: List of expense data dictionaries
    """
    batch_size = 50  # Process in smaller batches to avoid overwhelming the database
    for i in range(0, len(expenses), batch_size):
        batch = expenses[i : i + batch_size]
        await db.batch_create_records(user_id, dataset_id, batch)
        print(f"Loaded expenses {i+1} to {min(i+batch_size, len(expenses))}")


async def load_test_data(db: DatasetManager, user_id: str, scale: str = "medium") -> Dict[str, Any]:
    """Load test data at different scales.

    Args:
        db: Database manager instance
        user_id: User ID to associate with the data
        scale: Data volume scale - "small", "medium", "large", or "massive"

    Returns:
        Dict with dataset IDs and record counts
    """
    scales = {
        "small": {"tasks": 50, "expenses": 50},
        "medium": {"tasks": 200, "expenses": 200},
        "large": {"tasks": 1000, "expenses": 1000},
        "massive": {"tasks": 5000, "expenses": 5000},
    }

    count = scales.get(scale, scales["medium"])

    # Create datasets if they don't exist
    tasks_dataset_id = await create_tasks_dataset(db, user_id)
    expenses_dataset_id = await create_expenses_dataset(db, user_id)

    # Generate and load data
    print(f"Generating {count['tasks']} tasks...")
    tasks = generate_task_batch(count=count["tasks"])
    print(f"Generating {count['expenses']} expenses...")
    expenses = generate_expense_batch(count=count["expenses"])

    print("Loading tasks...")
    await load_task_batch(db, user_id, tasks_dataset_id, tasks)
    print("Loading expenses...")
    await load_expense_batch(db, user_id, expenses_dataset_id, expenses)

    print(f"Successfully loaded {count['tasks']} tasks and {count['expenses']} expenses")
    return {"tasks_dataset_id": tasks_dataset_id, "expenses_dataset_id": expenses_dataset_id, "task_count": count["tasks"], "expense_count": count["expenses"]}


async def main():
    """Main function to run the database initialization and data loading."""
    # Initialize MongoDB client
    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    client.get_io_loop = asyncio.get_running_loop

    try:
        # Setup database manager
        print("Setting up database manager...")
        db = await DatasetManager.setup(client)

        # Use a consistent user ID for testing
        user_id = "whatsapp:+971565312695"

        # Ask for the scale of data to load
        print("\nSelect data volume scale:")
        print("1. Small (50 tasks, 50 expenses)")
        print("2. Medium (200 tasks, 200 expenses)")
        print("3. Large (1000 tasks, 1000 expenses)")
        print("4. Massive (5000 tasks, 5000 expenses)")
        choice = input("Enter choice (1-4) [default=2]: ") or "2"

        scale_map = {"1": "small", "2": "medium", "3": "large", "4": "massive"}
        scale = scale_map.get(choice, "medium")

        # Load test data
        print(f"\nLoading {scale} scale test data...")
        dataset_info = await load_test_data(db, user_id, scale)

        print("\nTest data loaded successfully!")
        print("You can now test your assistant with the following datasets:")
        print(f"Tasks Dataset ID: {dataset_info['tasks_dataset_id']}")
        print(f"Expenses Dataset ID: {dataset_info['expenses_dataset_id']}")
        print(f"Total records: {dataset_info['task_count'] + dataset_info['expense_count']}")

    finally:
        # Close the client connection
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
