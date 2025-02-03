from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from database_manager.operations.base import DatabaseOperation
from database_manager.document import Document

if TYPE_CHECKING:
    from database_manager.collection import Collection
    from database_manager.database import Database


class DocumentInsertOperation(DatabaseOperation):
    def __init__(self, database: Database, collection: Collection, content: Dict[str, Any]) -> None:
        super().__init__(database)
        self.collection = collection
        self.content = content
        self.inserted_id: Optional[str] = None

    def execute(self) -> Document:
        self.collection.validate_document(self.content)
        # Create document dict with metadata
        doc = Document(content=self.content, collection=self.collection)
        mongo_doc = {**doc.to_dict(), "content": self.content}

        # Insert directly using PyMongo
        result = self.collection._mongo_collection.insert_one(mongo_doc)
        self.inserted_id = result.inserted_id
        return doc

    def undo(self) -> None:
        if self.inserted_id:
            self.collection._mongo_collection.delete_one({"_id": self.inserted_id})


class DocumentUpdateOperation(DatabaseOperation):
    def __init__(self, database: Database, document: Document, new_content: Dict[str, Any]) -> None:
        super().__init__(database)
        self.document = document
        self.old_content = document.content.copy()
        self.new_content = new_content

    def execute(self) -> bool:
        self.document.collection.validate_document(self.new_content)
        result = self.document.collection._mongo_collection.update_one({"_id": self.document.id}, {"$set": {"content": self.new_content}})
        if result.modified_count > 0:
            self.document.content = self.new_content
            return True
        return False

    def undo(self) -> None:
        self.document.collection._mongo_collection.update_one({"_id": self.document.id}, {"$set": {"content": self.old_content}})
        self.document.content = self.old_content


class DocumentDeleteOperation(DatabaseOperation):
    def __init__(self, database: Database, document: Document) -> None:
        super().__init__(database)
        self.document = document
        self.deleted_content = document.content.copy()

    def execute(self) -> bool:
        result = self.document.collection._mongo_collection.delete_one({"_id": self.document.id})
        return result.deleted_count > 0

    def undo(self) -> None:
        doc = {**self.document.to_dict(), "content": self.deleted_content}
        self.document.collection._mongo_collection.insert_one(doc)


class BulkUpdateOperation(DatabaseOperation):
    """Handles bulk update operations with undo support."""

    def __init__(self, database: Database, collection: Collection, original_states: List[Tuple[Document, Dict[str, Any]]], update_dict: Dict[str, Any]) -> None:
        super().__init__(database)
        self.collection = collection
        self.original_states = original_states
        self.update_dict = update_dict

    def execute(self) -> None:
        # Validate updates
        for doc, _ in self.original_states:
            self.collection.validate_document({**doc.content, **self.update_dict})

        # Apply updates
        doc_ids = [doc.id for doc, _ in self.original_states]
        self.collection._mongo_collection.update_many({"_id": {"$in": doc_ids}}, {"$set": {"content": self.update_dict}})

    def undo(self) -> None:
        # Restore original states one by one to ensure proper content structure
        for doc, original_content in self.original_states:
            self.collection._mongo_collection.update_one({"_id": doc.id}, {"$set": {"content": original_content}})


class BulkInsertOperation(DatabaseOperation):
    """Handles bulk insert operations with undo support."""

    def __init__(self, database: Database, collection: Collection, contents: List[Dict[str, Any]]) -> None:
        super().__init__(database)
        self.collection = collection
        self.contents = contents
        self.inserted_ids: List[str] = []

    def execute(self) -> List[Document]:
        # Validate all documents before insertion
        for content in self.contents:
            self.collection.validate_document(content)

        # Prepare documents for insertion
        docs = []
        mongo_docs = []
        for content in self.contents:
            doc = Document(content=content, collection=self.collection)
            mongo_doc = {**doc.to_dict(), "content": content}
            docs.append(doc)
            mongo_docs.append(mongo_doc)

        # Perform bulk insert
        result = self.collection._mongo_collection.insert_many(mongo_docs)
        self.inserted_ids = result.inserted_ids
        return docs

    def undo(self) -> None:
        if self.inserted_ids:
            self.collection._mongo_collection.delete_many({"_id": {"$in": self.inserted_ids}})


class BulkDeleteOperation(DatabaseOperation):
    """Handles bulk delete operations with undo support."""

    def __init__(self, database: Database, collection: Collection, deleted_docs: List[Tuple[Document, Dict[str, Any]]]) -> None:
        super().__init__(database)
        self.collection = collection
        self.deleted_docs = deleted_docs

    def execute(self) -> None:
        doc_ids = [doc.id for doc, _ in self.deleted_docs]
        self.collection._mongo_collection.delete_many({"_id": {"$in": doc_ids}})

    def undo(self) -> None:
        mongo_docs = [{**doc.to_dict(), "content": content} for doc, content in self.deleted_docs]
        self.collection._mongo_collection.insert_many(mongo_docs)
