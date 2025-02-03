from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple
from datetime import datetime, timezone
from enum import Enum
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from database_manager.database import Database
    from database_manager.collection import Collection
    from database_manager.document import Document


class OperationType(Enum):
    """Types of operations that can be performed on the database."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE_COLLECTION = "create_collection"
    DROP_COLLECTION = "drop_collection"
    RENAME_COLLECTION = "rename_collection"
    ADD_FIELDS = "add_fields"
    DELETE_FIELDS = "delete_fields"


@dataclass
class OperationState:
    """Represents the state of an operation for undo/redo purposes."""

    collection_name: str
    operation_type: OperationType
    document_id: Optional[str] = None
    old_state: Optional[Dict[str, Any]] = None
    new_state: Optional[Dict[str, Any]] = None
    collection_schema: Optional[Dict[str, Any]] = None
    collection_description: Optional[str] = None
    timestamp: datetime = datetime.now(timezone.utc)


class DatabaseOperation(ABC):
    """Abstract base class for database operations."""

    def __init__(self, database: Database) -> None:
        self.database = database

    @abstractmethod
    def execute(self) -> Any:
        """Execute the operation."""
        pass

    @abstractmethod
    def undo(self) -> None:
        """Undo the operation."""
        pass

    @abstractmethod
    def get_state(self) -> OperationState:
        """Get the operation's state."""
        pass


class OperationHistory:
    """Manages the history of database operations for undo/redo functionality."""

    def __init__(self, max_history: int = 100) -> None:
        self.history: List[OperationState] = []
        self.current_index: int = -1
        self.max_history = max_history

    def push(self, operation: OperationState) -> None:
        """Add a new operation to the history."""
        # Remove any redo operations
        if self.current_index < len(self.history) - 1:
            self.history = self.history[: self.current_index + 1]

        self.history.append(operation)
        self.current_index += 1

        # Maintain maximum history size
        if len(self.history) > self.max_history:
            self.history = self.history[1:]
            self.current_index -= 1

    def can_undo(self) -> bool:
        """Check if there are operations that can be undone."""
        return self.current_index >= 0

    def can_redo(self) -> bool:
        """Check if there are operations that can be redone."""
        return self.current_index < len(self.history) - 1

    def get_undo_operation(self) -> Optional[OperationState]:
        """Get the next operation to undo."""
        if not self.can_undo():
            return None
        operation = self.history[self.current_index]
        self.current_index -= 1
        return operation

    def get_redo_operation(self) -> Optional[OperationState]:
        """Get the next operation to redo."""
        if not self.can_redo():
            return None
        self.current_index += 1
        return self.history[self.current_index]


class DocumentInsertOperation(DatabaseOperation):
    def __init__(self, database: Database, collection: Collection, content: Dict[str, Any]) -> None:
        super().__init__(database)
        self.collection = collection
        self.content = content
        self.inserted_document: Optional[Document] = None

    def execute(self) -> Document:
        self.inserted_document = self.collection.insert_one(self.content)
        return self.inserted_document

    def undo(self) -> None:
        if self.inserted_document:
            self.inserted_document.delete()

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.collection.name,
            operation_type=OperationType.INSERT,
            document_id=str(self.inserted_document.id) if self.inserted_document else None,
            new_state=self.content,
        )


class DocumentUpdateOperation(DatabaseOperation):
    def __init__(self, database: Database, document: Document, new_content: Dict[str, Any]) -> None:
        super().__init__(database)
        self.document = document
        self.old_content = document.content.copy()
        self.new_content = new_content

    def execute(self) -> bool:
        self.document.content = self.new_content
        return self.document.update()

    def undo(self) -> None:
        self.document.content = self.old_content
        self.document.update()

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.document.collection.name,
            operation_type=OperationType.UPDATE,
            document_id=str(self.document.id),
            old_state=self.old_content,
            new_state=self.new_content,
        )


class DocumentDeleteOperation(DatabaseOperation):
    def __init__(self, database: Database, document: Document) -> None:
        super().__init__(database)
        self.document = document
        self.deleted_content = document.content.copy()

    def execute(self) -> bool:
        return self.document.delete()

    def undo(self) -> None:
        self.document.collection.insert_one(self.deleted_content)

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.document.collection.name,
            operation_type=OperationType.DELETE,
            document_id=str(self.document.id),
            old_state=self.deleted_content,
        )


class BulkUpdateOperation(DatabaseOperation):
    """Handles bulk update operations with undo support."""

    def __init__(self, database: Database, collection: Collection, original_states: List[Tuple[Document, Dict[str, Any]]], update_dict: Dict[str, Any]) -> None:
        super().__init__(database)
        self.collection = collection
        self.original_states = original_states
        self.update_dict = update_dict

    def execute(self) -> None:
        # Apply updates
        doc_ids = [doc.id for doc, _ in self.original_states]
        self.collection._mongo_collection.update_many({"_id": {"$in": doc_ids}}, {"$set": self.update_dict})

    def undo(self) -> None:
        # Restore original states
        for doc, original_content in self.original_states:
            self.collection._mongo_collection.replace_one({"_id": doc.id}, {**doc.to_dict(), "content": original_content})

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.collection.name,
            operation_type=OperationType.UPDATE,
            document_id=",".join(str(doc.id) for doc, _ in self.original_states),
            old_state={str(doc.id): content for doc, content in self.original_states},
            new_state=self.update_dict,
        )


class BulkDeleteOperation(DatabaseOperation):
    """Handles bulk delete operations with undo support."""

    def __init__(self, database: Database, collection: Collection, deleted_docs: List[Tuple[Document, Dict[str, Any]]]) -> None:
        super().__init__(database)
        self.collection = collection
        self.deleted_docs = deleted_docs

    def execute(self) -> None:
        # Delete documents
        doc_ids = [doc.id for doc, _ in self.deleted_docs]
        self.collection._mongo_collection.delete_many({"_id": {"$in": doc_ids}})

    def undo(self) -> None:
        # Restore deleted documents
        self.collection._mongo_collection.insert_many([{**doc.to_dict(), "content": content} for doc, content in self.deleted_docs])

    def get_state(self) -> OperationState:
        return OperationState(
            collection_name=self.collection.name,
            operation_type=OperationType.DELETE,
            document_id=",".join(str(doc.id) for doc, _ in self.deleted_docs),
            old_state={str(doc.id): content for doc, content in self.deleted_docs},
        )
