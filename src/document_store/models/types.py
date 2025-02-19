"""Type definitions for the document store module."""

from enum import Enum


class FieldType(str, Enum):
    """Supported field types for dataset schema."""

    INTEGER = "int"
    FLOAT = "float"
    STRING = "str"
    BOOLEAN = "bool"
    DATE = "date"
    DATETIME = "datetime"
    SELECT = "select"
    MULTI_SELECT = "multi_select"


# Safe type conversion mappings
SAFE_TYPE_CONVERSIONS = {
    FieldType.INTEGER: {FieldType.FLOAT, FieldType.STRING},
    FieldType.FLOAT: {FieldType.STRING},
    FieldType.BOOLEAN: {FieldType.STRING},
    FieldType.DATE: {FieldType.STRING, FieldType.DATETIME},
    FieldType.DATETIME: {FieldType.STRING},
    # SELECT and MULTI_SELECT only allow same type conversions
    FieldType.SELECT: {FieldType.SELECT},
    FieldType.MULTI_SELECT: {FieldType.MULTI_SELECT},
}
