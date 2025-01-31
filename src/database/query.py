from typing import Any, Dict, List, TYPE_CHECKING
from __future__ import annotations

if TYPE_CHECKING:
    from database.collection import Collection
    from database.document import Document


class Query:
    def __init__(self, collection: "Collection"):
        self.collection = collection
        self.filters = {}
        self.sort_fields = []
        self.limit_val = None

    def filter(self, conditions: Dict[str, Any]) -> "Query":
        self.filters.update(conditions)
        return self

    def sort(self, field: str, ascending: bool = True) -> "Query":
        self.sort_fields.append((field, 1 if ascending else -1))
        return self

    def limit(self, count: int) -> "Query":
        self.limit_val = count
        return self

    def execute(self) -> List["Document"]:
        return self.collection._execute_query(self)
