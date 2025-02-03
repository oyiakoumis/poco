from typing import List, Literal

from pydantic import Field, model_validator

from models.base import BaseTableOperation
from models.fields import ConditionModel, RecordModel

class AddRecordsModel(BaseTableOperation):
    """Model for adding records operation."""

    intent: Literal["add"]
    records: List[List[RecordModel]] = Field(min_items=1)


class UpdateRecordsModel(BaseTableOperation):
    """Model for updating records operation."""

    intent: Literal["update"]
    records: List[RecordModel] = Field(min_items=1)
    conditions: List[ConditionModel] = Field(default_factory=list)


class DeleteRecordsModel(BaseTableOperation):
    """Model for deleting records operation."""

    intent: Literal["delete"]
    conditions: List[ConditionModel] = Field(default_factory=list)
