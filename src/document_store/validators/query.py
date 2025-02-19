"""Validators for aggregation queries."""

from typing import List

from document_store.exceptions import InvalidRecordDataError
from document_store.models.query import (
    VALID_AGGREGATIONS,
    AggregationField,
    AggregationQuery,
)
from document_store.models.schema import DatasetSchema


def validate_aggregation_field(field: AggregationField, schema: DatasetSchema) -> None:
    """Validate aggregation field against schema.

    Args:
        field: Aggregation field to validate
        schema: Dataset schema

    Raises:
        InvalidRecordDataError: If field is invalid
    """
    try:
        schema_field = schema.get_field(field.field)
    except KeyError as e:
        raise InvalidRecordDataError(str(e))

    valid_ops = VALID_AGGREGATIONS.get(schema_field.type, set())
    if field.operation not in valid_ops:
        raise InvalidRecordDataError(f"Operation '{field.operation}' not valid for field type '{schema_field.type}'")


def validate_group_by_fields(fields: List[str], schema: DatasetSchema) -> None:
    """Validate group by fields exist in schema.

    Args:
        fields: List of field names to validate
        schema: Dataset schema

    Raises:
        InvalidRecordDataError: If any field is invalid
    """
    schema_fields = set(schema.get_field_names())
    invalid_fields = [f for f in fields if f not in schema_fields]
    if invalid_fields:
        raise InvalidRecordDataError(f"Invalid group by fields: {invalid_fields}")


def validate_aggregation_query(query: AggregationQuery, schema: DatasetSchema) -> None:
    """Validate complete aggregation query against schema.

    Args:
        query: Aggregation query to validate
        schema: Dataset schema

    Raises:
        InvalidRecordDataError: If query is invalid
    """
    # Validate group by fields if present
    if query.group_by:
        validate_group_by_fields(query.group_by, schema)

    # Validate each aggregation field
    for agg in query.aggregations:
        validate_aggregation_field(agg, schema)

    # Validate filter field if present
    if query.filter:
        if not schema.has_field(query.filter.field):
            raise InvalidRecordDataError(f"Filter field '{query.filter.field}' not found in schema")

    # Validate sort fields if present
    if query.sort:
        schema_fields = set(schema.get_field_names())
        agg_fields = {agg.alias for agg in query.aggregations}
        valid_sort_fields = schema_fields | agg_fields

        invalid_sort_fields = [f for f in query.sort.keys() if f not in valid_sort_fields]
        if invalid_sort_fields:
            raise InvalidRecordDataError(f"Invalid sort fields: {invalid_sort_fields}")
