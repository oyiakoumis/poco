from database_manager.collection import Collection
from database_manager.database import Database


from typing import Any, Dict, List, Optional

from database_manager.operations.base import DatabaseOperation, OperationState
from database_manager.operations.enums import OperationType
from database_manager.schema_field import SchemaField


class CreateCollectionOperation(DatabaseOperation):
    def __init__(self, database: Database, name: str, schema: Dict[str, Any], description: str) -> None:
        super().__init__(database)
        self.name = name
        self.schema = schema
        self.description = description
        self.created_collection: Optional[Collection] = None

    def execute(self) -> Collection:
        self.created_collection = self.database.create_collection(self.name, self.schema, self.description)
        return self.created_collection

    def undo(self) -> None:
        if self.created_collection:
            self.database.drop_collection(self.name)

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.name, operation_type=OperationType.CREATE_COLLECTION, collection_schema=self.schema, collection_description=self.description
        )


class DropCollectionOperation(DatabaseOperation):
    def __init__(self, database: Database, name: str) -> None:
        super().__init__(database)
        self.name = name
        self.schema = None
        self.description = None

    def execute(self) -> None:
        # Store collection info before dropping
        collection_def = self.database.registry.get_collection_definition(self.name)
        if collection_def:
            self.schema = collection_def.schema
            self.description = collection_def.description
        self.database.drop_collection(self.name)

    def undo(self) -> None:
        if self.schema and self.description:
            self.database.create_collection(self.name, self.schema, self.description)

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.name, operation_type=OperationType.DROP_COLLECTION, collection_schema=self.schema, collection_description=self.description
        )


class RenameCollectionOperation(DatabaseOperation):
    def __init__(self, database: Database, old_name: str, new_name: str) -> None:
        super().__init__(database)
        self.old_name = old_name
        self.new_name = new_name
        self.collection_def = None

    def execute(self) -> None:
        # Store collection info before renaming
        self.collection_def = self.database.registry.get_collection_definition(self.old_name)
        if not self.collection_def:
            raise ValueError(f"Collection '{self.old_name}' not found")

        # Use MongoDB's native rename operation
        self.database._mongo_db[self.old_name].rename(self.new_name)

        # Update the collection definition in registry
        self.collection_def.name = self.new_name
        self.database.registry.update_collection_definition(self.collection_def)

        # Update local collections cache
        self.database.collections[self.new_name] = self.database.collections.pop(self.old_name)
        self.database.collections[self.new_name].name = self.new_name

    def undo(self) -> None:
        # Reverse the rename using MongoDB's native operation
        self.database._mongo_db[self.new_name].rename(self.old_name)

        # Restore original collection definition
        self.collection_def.name = self.old_name
        self.database.registry.update_collection_definition(self.collection_def)

        # Update local collections cache
        self.database.collections[self.old_name] = self.database.collections.pop(self.new_name)
        self.database.collections[self.old_name].name = self.old_name

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.old_name,
            operation_type=OperationType.RENAME_COLLECTION,
            old_state={"name": self.old_name},
            new_state={"name": self.new_name},
            collection_schema=self.collection_def.schema if self.collection_def else None,
            collection_description=self.collection_def.description if self.collection_def else None,
        )


class AddFieldsOperation(DatabaseOperation):
    def __init__(self, database: Database, collection_name: str, new_fields: Dict[str, SchemaField]) -> None:
        super().__init__(database)
        self.collection_name = collection_name
        self.new_fields = new_fields
        self.collection_def = None

    def execute(self) -> None:
        self.collection_def = self.database.registry.get_collection_definition(self.collection_name)
        if not self.collection_def:
            raise ValueError(f"Collection '{self.collection_name}' not found")

        # Check for field name conflicts
        for field_name in self.new_fields:
            if field_name in self.collection_def.schema:
                raise ValueError(f"Field '{field_name}' already exists in collection")

        # Add new fields to schema
        updated_schema = self.collection_def.schema.copy()
        updated_schema.update(self.new_fields)

        # Update collection definition
        self.collection_def.schema = updated_schema
        self.database.registry.update_collection_definition(self.collection_def)

    def undo(self) -> None:
        if self.collection_def:
            # Remove added fields from schema
            updated_schema = {name: field for name, field in self.collection_def.schema.items() if name not in self.new_fields}
            self.collection_def.schema = updated_schema
            self.database.registry.update_collection_definition(self.collection_def)

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.collection_name,
            operation_type=OperationType.ADD_FIELDS,
            new_state={name: field.to_dict() for name, field in self.new_fields.items()},
        )


class DeleteFieldsOperation(DatabaseOperation):
    def __init__(self, database: Database, collection_name: str, field_names: List[str]) -> None:
        super().__init__(database)
        self.collection_name = collection_name
        self.field_names = field_names
        self.collection_def = None
        self.deleted_fields = {}

    def execute(self) -> None:
        self.collection_def = self.database.registry.get_collection_definition(self.collection_name)
        if not self.collection_def:
            raise ValueError(f"Collection '{self.collection_name}' not found")

        # Store fields before deletion
        self.deleted_fields = {name: self.collection_def.schema[name] for name in self.field_names if name in self.collection_def.schema}

        # Remove fields from schema
        updated_schema = {name: field for name, field in self.collection_def.schema.items() if name not in self.field_names}

        # Update collection definition
        self.collection_def.schema = updated_schema
        self.database.registry.update_collection_definition(self.collection_def)

        # Remove fields from all documents
        collection = self.database.collections[self.collection_name]
        query = collection.find()  # Create query
        documents = query.execute()  # Execute query to get actual documents

        for doc in documents:
            updated_content = {key: value for key, value in doc.content.items() if key not in self.field_names}
            doc.content = updated_content
            doc.update()

    def undo(self) -> None:
        if self.collection_def and self.deleted_fields:
            # Restore deleted fields to schema
            updated_schema = self.collection_def.schema.copy()
            updated_schema.update(self.deleted_fields)
            self.collection_def.schema = updated_schema
            self.database.registry.update_collection_definition(self.collection_def)

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.collection_name,
            operation_type=OperationType.DELETE_FIELDS,
            old_state={name: field.to_dict() for name, field in self.deleted_fields.items()},
            new_state={"deleted_fields": self.field_names},
        )
