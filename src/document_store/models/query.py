"""Query models for document store aggregations."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from document_store.exceptions import InvalidRecordDataError
from document_store.models.schema import DatasetSchema
from document_store.models.types import AggregationType, FieldType, TypeRegistry


class SortOrder(str, Enum):
    """Sort order options."""

    ASC = "asc"
    DESC = "desc"


class ComparisonOperator(str, Enum):
    """Supported comparison operators for filtering."""

    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_THAN_EQUALS = "gte"
    LESS_THAN = "lt"
    LESS_THAN_EQUALS = "lte"


class LogicalOperator(str, Enum):
    """Logical operators for combining filter conditions."""

    AND = "and"
    OR = "or"


class FilterCondition(BaseModel):
    """Single field condition."""

    field: str = Field(description="Field name to filter on")
    operator: ComparisonOperator
    value: Any = Field(description="Value to compare against")


class FilterExpression(BaseModel):
    """Logical combination of conditions or other expressions."""

    operator: LogicalOperator
    expressions: List[Union[FilterCondition, "FilterExpression"]]


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

    def validate_with_schema(self, schema: DatasetSchema) -> None:
        """Validate the field and operation against the schema."""
        try:
            schema_field = schema.get_field(self.field)
        except KeyError as e:
            raise InvalidRecordDataError(str(e))

        type_instance = TypeRegistry.get_type(schema_field.type)
        if not type_instance.can_aggregate(self.operation):
            raise InvalidRecordDataError(f"Operation '{self.operation}' not valid for field type '{schema_field.type}'")


class RecordQuery(BaseModel):
    """Query model for record operations.

    Supports both simple queries and aggregations:
    - Simple query: Use filter, sort, limit
    - Aggregation: Use group_by, aggregations, filter, sort, limit
    """

    group_by: Optional[List[str]] = Field(default=None, description="Fields to group by")
    aggregations: Optional[List[AggregationField]] = Field(default=None, description="Optional aggregation operations to perform")
    filter: Optional[Union[FilterCondition, FilterExpression]] = Field(default=None, description="Filter conditions to apply")
    sort: Optional[Dict[str, SortOrder]] = Field(default=None, description="Sorting configuration (field -> sort order)")
    limit: Optional[int] = Field(default=None, description="Maximum number of results to return")

    def validate_with_schema(self, schema: DatasetSchema) -> None:
        """Validate the query against a schema."""
        # Validate group by fields
        if self.group_by:
            schema_fields = set(schema.get_field_names())
            invalid_fields = [f for f in self.group_by if f not in schema_fields]
            if invalid_fields:
                raise InvalidRecordDataError(f"Invalid group by fields: {invalid_fields}")

        # Validate aggregations if present
        if self.aggregations:
            for agg in self.aggregations:
                agg.validate_with_schema(schema)

        # Validate filter conditions
        if self.filter:
            self._validate_filter_node(self.filter, schema)

    def _validate_filter_node(self, node: Union[FilterCondition, FilterExpression], schema: DatasetSchema) -> None:
        """Recursively validate filter nodes against schema."""
        if isinstance(node, FilterCondition):
            # Validate single condition
            if not schema.has_field(node.field):
                raise InvalidRecordDataError(f"Filter field '{node.field}' not found in schema")

            # Validate filter value against field type
            field = schema.get_field(node.field)
            type_impl = TypeRegistry.get_type(field.type)

            # Set options for select/multi-select fields before validation
            if field.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
                if not field.options:
                    raise InvalidRecordDataError(f"Options not provided for {field.type} field '{field.field_name}'")
                type_impl.set_options(field.options)

            try:
                # Convert the filter value using the field's type implementation
                node.value = type_impl.validate(node.value)
            except ValueError as e:
                raise InvalidRecordDataError(f"Invalid filter value for field '{node.field}': {str(e)}")
        else:
            # Validate nested expressions
            if not node.expressions:
                raise InvalidRecordDataError("Filter expression must contain at least one condition")
            for expr in node.expressions:
                self._validate_filter_node(expr, schema)

        # Validate sort fields
        if self.sort:
            schema_fields = set(schema.get_field_names())
            agg_fields = set()
            if self.aggregations:
                agg_fields = {agg.alias for agg in self.aggregations}
            valid_sort_fields = schema_fields | agg_fields

            invalid_sort_fields = [f for f in self.sort.keys() if f not in valid_sort_fields]
            if invalid_sort_fields:
                raise InvalidRecordDataError(f"Invalid sort fields: {invalid_sort_fields}")

    class Config:
        arbitrary_types_allowed = True
        json_schema_extra = {
            "examples": [
                # Simple query example with multiple conditions
                {
                    "filter": {
                        "operator": "and",
                        "expressions": [{"field": "status", "operator": "eq", "value": "active"}, {"field": "age", "operator": "gte", "value": 18}],
                    },
                    "sort": {"created_at": "desc"},
                    "limit": 10,
                },
                # Aggregation query example
                {
                    "group_by": ["category"],
                    "aggregations": [
                        {"field": "amount", "operation": "sum"},
                        {"field": "amount", "operation": "avg", "alias": "average_amount"},
                        {"field": "id", "operation": "count", "alias": "total_records"},
                    ],
                    "filter": {"field": "status", "operator": "eq", "value": "completed"},
                    "sort": {"amount_sum": "desc"},
                    "limit": 10,
                },
            ]
        }
