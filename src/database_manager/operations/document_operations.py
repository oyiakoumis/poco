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

    async def execute(self) -> Document:
        self.collection.validate_document(self.content)
        # Create document dict with metadata
        doc = Document(content=self.content, collection=self.collection)
        mongo_doc = {**(await doc.to_dict()), "content": self.content}

        # Insert directly using Motor
        result = await self.collection._mongo_collection.insert_one(mongo_doc)
        self.inserted_id = result.inserted_id
        return doc

    async def undo(self) -> None:
        if self.inserted_id:
            await self.collection._mongo_collection.delete_one({"_id": self.inserted_id})


class DocumentUpdateOperation(DatabaseOperation):
    def __init__(self, database: Database, document: Document, new_content: Dict[str, Any]) -> None:
        super().__init__(database)
        self.document = document
        self.old_content = document.content.copy()
        self.new_content = new_content

    async def execute(self) -> bool:
        self.document.collection.validate_document(self.new_content)
        result = await self.document.collection._mongo_collection.update_one({"_id": self.document.id}, {"$set": {"content": self.new_content}})
        if result.modified_count > 0:
            self.document.content = self.new_content
            return True
        return False

    async def undo(self) -> None:
        await self.document.collection._mongo_collection.update_one({"_id": self.document.id}, {"$set": {"content": self.old_content}})
        self.document.content = self.old_content


class DocumentDeleteOperation(DatabaseOperation):
    def __init__(self, database: Database, document: Document) -> None:
        super().__init__(database)
        self.document = document
        self.deleted_content = document.content.copy()

    async def execute(self) -> bool:
        result = await self.document.collection._mongo_collection.delete_one({"_id": self.document.id})
        return result.deleted_count > 0

    async def undo(self) -> None:
        doc = {**(await self.document.to_dict()), "content": self.deleted_content}
        await self.document.collection._mongo_collection.insert_one(doc)


class BulkUpdateOperation(DatabaseOperation):
    """Handles bulk update operations with undo support."""

    def __init__(self, database: Database, collection: Collection, original_states: List[Tuple[Document, Dict[str, Any]]], update_dict: Dict[str, Any]) -> None:
        super().__init__(database)
        self.collection = collection
        self.original_states = original_states
        self.update_dict = update_dict

    async def execute(self) -> None:
        # Validate updates
        for doc, _ in self.original_states:
            self.collection.validate_document({**doc.content, **self.update_dict})

        # Apply updates
        doc_ids = [doc.id for doc, _ in self.original_states]
        await self.collection._mongo_collection.update_many({"_id": {"$in": doc_ids}}, {"$set": {"content": self.update_dict}})

    async def undo(self) -> None:
        # Restore original states one by one to ensure proper content structure
        for doc, original_content in self.original_states:
            await self.collection._mongo_collection.update_one({"_id": doc.id}, {"$set": {"content": original_content}})


class BulkInsertOperation(DatabaseOperation):
    """Handles bulk insert operations with undo support."""

    def __init__(self, database: Database, collection: Collection, contents: List[Dict[str, Any]]) -> None:
        super().__init__(database)
        self.collection = collection
        self.contents = contents
        self.inserted_ids: List[str] = []

    async def execute(self) -> List[Document]:
        # Validate all documents before insertion
        for content in self.contents:
            self.collection.validate_document(content)

        # Prepare documents for insertion
        docs = []
        mongo_docs = []
        for content in self.contents:
            doc = Document(content=content, collection=self.collection)
            mongo_doc = {**(await doc.to_dict()), "content": content}
            docs.append(doc)
            mongo_docs.append(mongo_doc)

        # Perform bulk insert
        result = await self.collection._mongo_collection.insert_many(mongo_docs)
        self.inserted_ids = result.inserted_ids
        return docs

    async def undo(self) -> None:
        if self.inserted_ids:
            await self.collection._mongo_collection.delete_many({"_id": {"$in": self.inserted_ids}})


class BulkDeleteOperation(DatabaseOperation):
    """Handles bulk delete operations with undo support."""

    def __init__(self, database: Database, collection: Collection, deleted_docs: List[Tuple[Document, Dict[str, Any]]]) -> None:
        super().__init__(database)
        self.collection = collection
        self.deleted_docs = deleted_docs

    async def execute(self) -> None:
        doc_ids = [doc.id for doc, _ in self.deleted_docs]
        await self.collection._mongo_collection.delete_many({"_id": {"$in": doc_ids}})

    async def undo(self) -> None:
        mongo_docs = []
        for doc, content in self.deleted_docs:
            doc_dict = await doc.to_dict()
            mongo_docs.append({**doc_dict, "content": content})
        await self.collection._mongo_collection.insert_many(mongo_docs)
