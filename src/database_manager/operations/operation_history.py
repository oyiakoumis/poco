from typing import List, Optional

from database_manager.operations.base import DatabaseOperation


class OperationHistory:
    """Manages the history of database operations for undo/redo functionality."""

    def __init__(self, max_history: int = 100) -> None:
        self.history: List[DatabaseOperation] = []  # Store actual operations
        self.current_index: int = -1
        self.max_history = max_history

    def push(self, operation: DatabaseOperation) -> None:
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

    def get_undo_operation(self) -> Optional[DatabaseOperation]:
        """Get the next operation to undo."""
        if not self.can_undo():
            return None
        operation = self.history[self.current_index]
        self.current_index -= 1
        return operation

    def get_redo_operation(self) -> Optional[DatabaseOperation]:
        """Get the next operation to redo."""
        if not self.can_redo():
            return None
        self.current_index += 1
        return self.history[self.current_index]
