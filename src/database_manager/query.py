from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional
from database_manager.document import Document

if TYPE_CHECKING:
    from database_manager.collection import Collection


class AggregateFn(str, Enum):
    """Enumeration for supported aggregate functions."""

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class Query:
    """A helper class to build and execute queries against a collection."""

    def __init__(self, collection: "Collection") -> None:
        self.collection = collection
        self.filters: Dict[str, Any] = {}
        self.sort_fields: List[tuple[str, int]] = []
        self.limit_val: int | None = None
        self.group_fields: List[str] = []
        self.aggregations: List[Dict[str, Any]] = []

    def filter(self, conditions: Dict[str, Any]) -> Query:
        """Add filtering conditions to the query."""
        self.filters.update(conditions)
        return self

    def sort(self, field: str, ascending: bool = True) -> Query:
        """Add sorting for a given field."""
        self.sort_fields.append((field, 1 if ascending else -1))
        return self

    def limit(self, count: int) -> Query:
        """Limit the number of results."""
        self.limit_val = count
        return self

    def group_by(self, *fields: str) -> Query:
        """Group results by specified fields."""
        self.group_fields.extend(fields)
        return self

    def aggregate(self, field: str, operation: AggregateFn, alias: Optional[str] = None) -> Query:
        """Add an aggregation operation to the query."""
        self.aggregations.append({"field": field, "operation": operation, "alias": alias or f"{field}_{operation}"})
        return self

    def _build_aggregation_pipeline(self) -> List[Dict[str, Any]]:
        """Build the MongoDB aggregation pipeline."""
        pipeline = []

        if self.filters:
            pipeline.append({"$match": self.filters})

        if self.group_fields or self.aggregations:
            group_stage = {"$group": {"_id": {field: f"${field}" for field in self.group_fields} if self.group_fields else None}}

            # Add aggregations
            for agg in self.aggregations:
                field, op, alias = agg["field"], agg["operation"], agg["alias"]
                if op == "count":
                    group_stage["$group"][alias] = {"$sum": 1}
                else:
                    group_stage["$group"][alias] = {f"${op}": f"${field}"}

            pipeline.append(group_stage)

            # Flatten results if using group_by
            if self.group_fields:
                project = {"_id": 0}
                for field in self.group_fields:
                    project[field] = f"$_id.{field}"
                for agg in self.aggregations:
                    project[agg["alias"]] = 1
                pipeline.append({"$project": project})

        if self.sort_fields:
            pipeline.append({"$sort": dict(self.sort_fields)})

        if self.limit_val:
            pipeline.append({"$limit": self.limit_val})

        return pipeline

    def execute(self) -> List[Dict[str, Any]] | List[Document]:
        """Execute the query and return results."""
        if not self.group_fields and not self.aggregations:
            return self._execute_find()
        return list(self.collection._mongo_collection.aggregate(self._build_aggregation_pipeline()))

    def _execute_find(self) -> List[Document]:
        """Execute a regular find query."""
        cursor = self.collection._mongo_collection.find(self.filters)

        if self.sort_fields:
            cursor = cursor.sort(self.sort_fields)
        if self.limit_val:
            cursor = cursor.limit(self.limit_val)

        return [Document.from_dict(doc, self.collection) for doc in cursor]
