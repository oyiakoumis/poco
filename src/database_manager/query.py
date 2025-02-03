from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from database_manager.collection import Collection
    from database_manager.document import Document


class Query:
    """
    A helper class to build and execute queries against a collection.
    """

    def __init__(self, collection: "Collection") -> None:
        self.collection = collection
        self.filters: Dict[str, Any] = {}
        self.sort_fields: List[tuple[str, int]] = []
        self.limit_val: int | None = None

    def filter(self, conditions: Dict[str, Any]) -> Query:
        """
        Add filtering conditions to the query.
        """
        self.filters.update(conditions)
        return self

    def sort(self, field: str, ascending: bool = True) -> Query:
        """
        Add sorting for a given field.
        """
        self.sort_fields.append((field, 1 if ascending else -1))
        return self

    def limit(self, count: int) -> Query:
        """
        Limit the number of results.
        """
        self.limit_val = count
        return self

    def execute(self) -> List["Document"]:
        """
        Execute the query and return a list of Document instances.
        """
        return self.collection._execute_query(self)
