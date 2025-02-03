from enum import Enum


class OperationType(Enum):
    """Types of operations that can be performed on the database."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE_COLLECTION = "create_collection"
    DROP_COLLECTION = "drop_collection"
    RENAME_COLLECTION = "rename_collection"
    ADD_FIELDS = "add_fields"
    DELETE_FIELDS = "delete_fields"
