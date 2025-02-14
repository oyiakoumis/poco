"""Custom exceptions for the document store module."""


class DocumentStoreError(Exception):
    """Base exception for document store errors."""

    pass


class ValidationError(DocumentStoreError):
    """Raised when data validation fails."""

    pass


class DatasetError(DocumentStoreError):
    """Base exception for dataset-related errors."""

    pass


class DatasetNotFoundError(DatasetError):
    """Raised when a dataset is not found."""

    pass


class DatasetNameExistsError(DatasetError):
    """Raised when attempting to create a dataset with a name that already exists."""

    pass


class InvalidDatasetSchemaError(DatasetError):
    """Raised when the dataset schema is invalid."""

    pass


class RecordError(DocumentStoreError):
    """Base exception for record-related errors."""

    pass


class RecordNotFoundError(RecordError):
    """Raised when a record is not found."""

    pass


class InvalidRecordDataError(RecordError):
    """Raised when record data does not match the dataset schema."""

    pass


class DatabaseError(DocumentStoreError):
    """Raised when a database operation fails."""

    pass


class UserError(DocumentStoreError):
    """Raised when a user-related operation fails."""

    pass


class InvalidFieldTypeError(ValidationError):
    """Raised when a field type is not supported."""

    pass


class InvalidFieldValueError(ValidationError):
    """Raised when a field value does not match its defined type."""

    pass


class InvalidSchemaUpdateError(ValidationError):
    """Raised when a schema update is invalid."""

    pass


class TypeConversionError(ValidationError):
    """Raised when type conversion fails."""

    pass
