"""Record data validation functions."""

from typing import Any, Dict

from document_store.exceptions import InvalidRecordDataError
from document_store.models.schema import DatasetSchema


def validate_query_fields(query: Dict[str, Any], schema: DatasetSchema) -> None:
    """Validate that query fields exist in dataset schema.

    Args:
        query: Query dictionary
        schema: Dataset schema to validate against

    Raises:
        InvalidRecordDataError: If query contains unknown fields
    """
    field_names = {field.field_name for field in schema}
    query_fields = set(query.keys())
    unknown_fields = query_fields - field_names

    if unknown_fields:
        raise InvalidRecordDataError(f"Query contains unknown fields: {', '.join(unknown_fields)}")
