from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from database_manager.database import Database


class DatabaseOperation(ABC):
    """Abstract base class for database operations."""

    def __init__(self, database: Database) -> None:
        self.database = database

    @abstractmethod
    async def execute(self) -> Any:
        """Execute the operation."""
        pass

    @abstractmethod
    async def undo(self) -> None:
        """Undo the operation."""
        pass
