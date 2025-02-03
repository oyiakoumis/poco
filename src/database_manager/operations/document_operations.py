from __future__ import annotations
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple

from database_manager.operations.base import DatabaseOperation, OperationState
from database_manager.operations.enums import OperationType

if TYPE_CHECKING:
    from database_manager.database import Database
    from database_manager.collection import Collection
    from database_manager.document import Document


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
