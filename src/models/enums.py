from enum import Enum


class SortDirection(str, Enum):
    """Enumeration for sort directions to ensure type safety."""

    ASC = "ASC"
    DESC = "DESC"


class ComparisonOperator(str, Enum):
    """Enumeration for comparison operators."""

    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
    CONTAINS = "contains"


class IndexOrder(str, Enum):
    """Enumeration for index ordering."""

    ASCENDING = "1"
    DESCENDING = "-1"
    TEXT = "text"
