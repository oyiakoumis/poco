"""Query models for document store aggregations."""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from document_store.models.types import FieldType


class ComparisonOperator(str, Enum):
    """Supported comparison operators for filtering."""

    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_THAN_EQUALS = "gte"
    LESS_THAN = "lt"
    LESS_THAN_EQUALS = "lte"


class AggregationType(str, Enum):
    """Supported aggregation operations."""

    SUM = "sum"  # For INTEGER, FLOAT
    AVG = "avg"  # For INTEGER, FLOAT
    MIN = "min"  # For INTEGER, FLOAT, DATE, DATETIME
    MAX = "max"  # For INTEGER, FLOAT, DATE, DATETIME
    COUNT = "count"  # For any type


# Mapping of which aggregations are valid for which field types
VALID_AGGREGATIONS = {
    FieldType.INTEGER: {AggregationType.SUM, AggregationType.AVG, AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.FLOAT: {AggregationType.SUM, AggregationType.AVG, AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.STRING: {AggregationType.COUNT},
    FieldType.BOOLEAN: {AggregationType.COUNT},
    FieldType.DATE: {AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.DATETIME: {AggregationType.MIN, AggregationType.MAX, AggregationType.COUNT},
    FieldType.SELECT: {AggregationType.COUNT},
    FieldType.MULTI_SELECT: {AggregationType.COUNT},
}


class FilterCondition(BaseModel):
    """Single filter condition."""

    operator: ComparisonOperator
    value: Any


class FilterExpression(BaseModel):
    """Filter expression for a single field."""

    field: str
    condition: FilterCondition


class AggregationField(BaseModel):
    """Defines an aggregation operation on a field."""

    field: str
    operation: AggregationType
    alias: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.alias is None:
            self.alias = f"{self.field}_{self.operation}"


class AggregationQuery(BaseModel):
    """Query model for aggregation operations."""

    group_by: Optional[List[str]] = Field(default=None, description="Fields to group by")
    aggregations: List[AggregationField] = Field(..., description="Aggregation operations to perform")  # Required
    filter: Optional[FilterExpression] = Field(default=None, description="Filter conditions to apply before aggregation")
    sort: Optional[Dict[str, bool]] = Field(default=None, description="Sorting configuration (field -> ascending)")
    limit: Optional[int] = Field(default=None, description="Maximum number of results to return")

    class Config:
        json_schema_extra = {
            "example": {
                "group_by": ["category"],
                "aggregations": [
                    {"field": "amount", "operation": "sum"},
                    {"field": "amount", "operation": "avg", "alias": "average_amount"},
                    {"field": "id", "operation": "count", "alias": "total_records"},
                ],
                "filter": {"field": "status", "condition": {"operator": "eq", "value": "completed"}},
                "sort": {"amount_sum": False},  # Sort by sum descending
                "limit": 10,
            }
        }
