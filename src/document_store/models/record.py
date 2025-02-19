"""Record model for document store."""

from typing import Any, Dict

from document_store.models.base import BaseDocument
from document_store.models.types import PydanticObjectId

RecordData = Dict[str, Any]


class Record(BaseDocument):
    """Record model representing a single document in a dataset."""

    dataset_id: PydanticObjectId
    data: RecordData
