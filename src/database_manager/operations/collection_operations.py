from database_manager.collection import Collection
from database_manager.collection_definition import CollectionDefinition
from database_manager.database import Database


from typing import Any, Dict, List, Optional

from database_manager.operations.base import DatabaseOperation
from database_manager.schema_field import SchemaField


class CreateCollectionOperation(DatabaseOperation):
    def __init__(self, database: Database, name: str, schema: Dict[str, Any], description: str) -> None:
        super().__init__(database)
        self.name = name
        self.schema = schema
        self.description = description
        self.created_collection: Optional[Collection] = None

    def execute(self) -> Collection:
        # Create definition first
        definition = CollectionDefinition(self.name, self.database.registry, self.description, self.schema)
        self.database.registry.register_collection(definition)

        # Create actual collection
        collection = Collection(self.name, self, self.database.embeddings, self.schema)
        collection.create_collection()
        return collection

    def undo(self) -> None:
        if self.created_collection:
            self.database.drop_collection(self.name)


class DropCollectionOperation(DatabaseOperation):
    def __init__(self, database: Database, name: str) -> None:
        super().__init__(database)
        self.name = name
        self.schema = None
        self.description = None

    def execute(self) -> None:
        # Store collection info before dropping
        collection_def = self.database.registry.get_collection_definition(self.name)
        self.schema = collection_def.schema
        self.description = collection_def.description

        if collection_def is not None:
            # Drop the actual collection
            self.database._mongo_db.drop_collection(self.name)
            # Remove from registry
            self.database.registry.unregister_collection(self.name)

    def undo(self) -> None:
        if self.schema and self.description:
            self.database.create_collection(self.name, self.schema, self.description)


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

    def undo(self) -> None:
        # Reverse the rename using MongoDB's native operation
        self.database._mongo_db[self.new_name].rename(self.old_name)

        # Restore original collection definition
        self.collection_def.name = self.old_name
        self.database.registry.update_collection_definition(self.collection_def)


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
        collection = self.database.get_collection(self.collection_def)
        documents = collection.get_all_documents()

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
