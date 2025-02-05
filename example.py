import asyncio
from datetime import datetime

from models.schema import (
    AggregateFunction,
    AggregateMetric,
    AggregationQuery,
    CollectionSchema,
    DocumentQuery,
    FieldDefinition,
    FieldType,
)
from database_manager.document_db import DocumentDB


async def main():
    # Initialize database
    from constants import DATABASE_CONNECTION_STRING
    from motor.motor_asyncio import AsyncIOMotorClient

    client = AsyncIOMotorClient(DATABASE_CONNECTION_STRING)
    database = client.get_database("example_db")
    db = DocumentDB(database)

    # Create a schema for a tasks collection
    tasks_schema = CollectionSchema(
        name="tasks",
        description="Collection for tracking tasks",
        fields=[
            FieldDefinition(
                name="title",
                description="Task title",
                field_type=FieldType.STRING,
                required=True,
            ),
            FieldDefinition(
                name="description",
                description="Task description",
                field_type=FieldType.STRING,
                required=False,
            ),
            FieldDefinition(
                name="priority",
                description="Task priority",
                field_type=FieldType.SELECT,
                options=["low", "medium", "high"],
                default="medium",
            ),
            FieldDefinition(
                name="tags",
                description="Task tags",
                field_type=FieldType.MULTI_SELECT,
                options=["work", "personal", "urgent", "project"],
            ),
            FieldDefinition(
                name="due_date",
                description="Task due date",
                field_type=FieldType.DATETIME,
            ),
            FieldDefinition(
                name="completed",
                description="Whether the task is completed",
                field_type=FieldType.BOOLEAN,
                default=False,
            ),
            FieldDefinition(
                name="effort_hours",
                description="Estimated effort in hours",
                field_type=FieldType.FLOAT,
                default=1.0,
            ),
        ],
    )

    # Create collection
    user_id = "example_user"
    try:
        # Try to delete existing collection first
        try:
            await db.delete_collection(user_id, "tasks")
            print("Deleted existing tasks collection")
        except ValueError:
            pass  # Collection didn't exist, which is fine
            
        await db.create_collection(user_id, tasks_schema)
        print("Created tasks collection")
    except Exception as e:
        print(f"Error creating collection: {e}")
        return

    # Create some sample tasks
    tasks = [
        {
            "title": "Complete project proposal",
            "description": "Write and submit the Q1 project proposal",
            "priority": "high",
            "tags": ["work", "project"],
            "due_date": datetime(2024, 3, 1),
            "effort_hours": 4.5,
        },
        {
            "title": "Weekly team meeting",
            "description": "Regular team sync-up",
            "priority": "medium",
            "tags": ["work"],
            "due_date": datetime(2024, 2, 15),
            "effort_hours": 1.0,
        },
        {
            "title": "Gym session",
            "description": "Regular workout",
            "priority": "low",
            "tags": ["personal"],
            "due_date": datetime(2024, 2, 10),
            "effort_hours": 2.0,
        },
    ]

    # Add tasks
    for task in tasks:
        doc_id = await db.create_document(user_id, "tasks", task)
        print(f"Created task: {task['title']} (ID: {doc_id})")

    # Query tasks
    print("\nAll tasks:")
    all_tasks = await db.query_documents(user_id, "tasks", DocumentQuery())
    for task in all_tasks:
        print(f"- {task['title']} (Priority: {task['priority']})")

    # Query high priority tasks
    print("\nHigh priority tasks:")
    query = DocumentQuery(filter={"priority": "high"})
    high_priority = await db.query_documents(user_id, "tasks", query)
    for task in high_priority:
        print(f"- {task['title']}")

    # Aggregate tasks by priority
    print("\nEffort hours by priority:")
    agg_query = AggregationQuery(
        group_by=["priority"],
        metrics=[
            AggregateMetric(field="effort_hours", function=AggregateFunction.SUM),
            AggregateMetric(field="effort_hours", function=AggregateFunction.AVERAGE),
        ],
    )
    aggregation = await db.aggregate_documents(user_id, "tasks", agg_query)
    for result in aggregation:
        priority = result["_id"]["priority"]
        total = result["effort_hours_sum"]
        avg = result["effort_hours_avg"]
        print(f"- {priority}: total={total:.1f}h, avg={avg:.1f}h")


if __name__ == "__main__":
    asyncio.run(main())
