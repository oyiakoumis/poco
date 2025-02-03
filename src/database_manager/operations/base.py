from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from database_manager.database import Database
from database_manager.operations.enums import OperationType


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
