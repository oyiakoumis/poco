"""Query models for document store aggregations."""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from document_store.exceptions import InvalidRecordDataError
from document_store.models.types import AggregationType, TypeRegistry
from document_store.models.schema import DatasetSchema


class ComparisonOperator(str, Enum):
    """Supported comparison operators for filtering."""

    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_THAN_EQUALS = "gte"
    LESS_THAN = "lt"
    LESS_THAN_EQUALS = "lte"


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

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, **data):
        super().__init__(**data)
        if self.alias is None:
            self.alias = f"{self.field}_{self.operation}"

    @model_validator(mode="after")
    def validate_field_operation(self) -> "AggregationField":
        """Validate the field and operation against the schema."""
        # Note: This will be called during AggregationQuery validation
        # where schema will be available in the context
        schema: DatasetSchema = self.model_extra.get("schema")
        if not schema:
            return self

        try:
            schema_field = schema.get_field(self.field)
        except KeyError as e:
            raise InvalidRecordDataError(str(e))

        type_instance = TypeRegistry.get_type(schema_field.type)
        if not type_instance.can_aggregate(self.operation):
            raise InvalidRecordDataError(f"Operation '{self.operation}' not valid for field type '{schema_field.type}'")

        return self


class AggregationQuery(BaseModel):
    """Query model for aggregation operations."""

    group_by: Optional[List[str]] = Field(default=None, description="Fields to group by")
    aggregations: List[AggregationField] = Field(..., description="Aggregation operations to perform")  # Required
    filter: Optional[FilterExpression] = Field(default=None, description="Filter conditions to apply before aggregation")
    sort: Optional[Dict[str, bool]] = Field(default=None, description="Sorting configuration (field -> ascending)")
    limit: Optional[int] = Field(default=None, description="Maximum number of results to return")

    def validate_with_schema(self, schema: DatasetSchema) -> None:
        """Validate the query against a schema."""
        # Store schema in context for child validators
        self.model_extra = {"schema": schema}

        # Validate group by fields
        if self.group_by:
            schema_fields = set(schema.get_field_names())
            invalid_fields = [f for f in self.group_by if f not in schema_fields]
            if invalid_fields:
                raise InvalidRecordDataError(f"Invalid group by fields: {invalid_fields}")

        # Validate aggregations
        for agg in self.aggregations:
            agg.model_extra = {"schema": schema}
            agg.validate_field_operation()

        # Validate filter field
        if self.filter and not schema.has_field(self.filter.field):
            raise InvalidRecordDataError(f"Filter field '{self.filter.field}' not found in schema")

        # Validate sort fields
        if self.sort:
            schema_fields = set(schema.get_field_names())
            agg_fields = {agg.alias for agg in self.aggregations}
            valid_sort_fields = schema_fields | agg_fields

            invalid_sort_fields = [f for f in self.sort.keys() if f not in valid_sort_fields]
            if invalid_sort_fields:
                raise InvalidRecordDataError(f"Invalid sort fields: {invalid_sort_fields}")

    class Config:
        arbitrary_types_allowed = True
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
