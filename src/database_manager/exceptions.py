class DocumentDBError(Exception):
    """Base exception for document database errors."""
    pass


class SchemaValidationError(DocumentDBError):
    """Raised when document validation against schema fails."""
    pass


class DocumentNotFoundError(DocumentDBError):
    """Raised when a document is not found."""
    pass


class CollectionNotFoundError(DocumentDBError):
    """Raised when a collection is not found."""
    pass


class DuplicateCollectionError(DocumentDBError):
    """Raised when attempting to create a collection that already exists."""
    pass


class UnauthorizedAccessError(DocumentDBError):
    """Raised when attempting to access a collection without proper authorization."""
    pass


class InvalidQueryError(DocumentDBError):
    """Raised when a query is invalid."""
    pass


class InvalidAggregationError(DocumentDBError):
    """Raised when an aggregation query is invalid."""
    pass
