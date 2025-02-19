"""Record model for document store."""

from typing import Any, Dict

from pydantic import model_validator

from document_store.exceptions import InvalidFieldValueError, InvalidRecordDataError
from document_store.models.base import BaseDocument
from document_store.models.schema import DatasetSchema
from document_store.models.types import FieldType, PydanticObjectId
from document_store.type_validators.factory import get_validator

RecordData = Dict[str, Any]


class Record(BaseDocument):
    """Record model representing a single document in a dataset."""

    dataset_id: PydanticObjectId
    data: RecordData

    @staticmethod
    def validate_data(data: RecordData, schema: DatasetSchema) -> RecordData:
        """Validate record data against dataset schema.

        Args:
            data: Record data to validate
            schema: Dataset schema to validate against

        Returns:
            RecordData: The validated data

        Raises:
            InvalidRecordDataError: If data doesn't match schema
            InvalidFieldValueError: If field value doesn't match type
        """
        validated_data = {}
        field_map = {field.field_name: field for field in schema}

        # Check for unknown fields
        unknown_fields = set(data.keys()) - set(field_map.keys())
        if unknown_fields:
            raise InvalidRecordDataError(f"Unknown fields in record data: {', '.join(unknown_fields)}")

        # Check required fields and validate types
        for field in schema:
            value = data.get(field.field_name)
            validator = get_validator(field.type)

            # Handle required fields
            if field.required and value is None:
                if field.default is not None:
                    value = field.default
                else:
                    raise InvalidRecordDataError(f"Required field '{field.field_name}' is missing")

            # Skip optional fields with no value
            if value is None:
                if field.default is not None:
                    # Set options for select/multi-select fields
                    if field.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
                        if not field.options:
                            raise InvalidFieldValueError(f"Options not provided for {field.type} field '{field.field_name}'")
                        validator.set_options(field.options)
                    try:
                        validated_data[field.field_name] = validator.validate_default(field.default)
                    except ValueError as e:
                        raise InvalidFieldValueError(f"Invalid default value for field '{field.field_name}': {str(e)}")
                continue

            # Set options for select/multi-select fields
            if field.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
                if not field.options:
                    raise InvalidFieldValueError(f"Options not provided for {field.type} field '{field.field_name}'")
                validator.set_options(field.options)

            # Validate and convert field value
            try:
                validated_data[field.field_name] = validator.validate(value)
            except ValueError as e:
                raise InvalidFieldValueError(f"Invalid value for field '{field.field_name}': {str(e)}")

        return validated_data


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
