from document_store.exceptions import InvalidDatasetSchemaError
from document_store.models.schema import DatasetSchema
from document_store.models.types import FieldType
from document_store.validators.factory import get_validator


def validate_schema(schema: DatasetSchema) -> DatasetSchema:
    """Validate dataset schema.

    Args:
        schema: Schema to validate

    Returns:
        Validated schema

    Raises:
        InvalidDatasetSchemaError: If schema is invalid
    """
    # Check for duplicate field names
    field_names = [field.field_name for field in schema]
    if len(field_names) != len(set(field_names)):
        raise InvalidDatasetSchemaError("Duplicate field names in schema")

    # Validate field types and default values
    for field in schema:
        if not isinstance(field.type, FieldType):
            try:
                field.type = FieldType(field.type)
            except ValueError:
                raise InvalidDatasetSchemaError(f"Invalid field type: {field.type}")

        validator = get_validator(field.type)
        if field.type in (FieldType.SELECT, FieldType.MULTI_SELECT):
            if not field.options:
                raise InvalidDatasetSchemaError(f"Options not provided for {field.type} field '{field.field_name}'")
            validator.set_options(field.options)

        # Validate default value using appropriate validator
        if field.default is not None:
            try:
                field.default = validator.validate_default(field.default)
            except ValueError as e:
                raise InvalidDatasetSchemaError(f"Invalid default value for field '{field.field_name}': {str(e)}")

    return schema
