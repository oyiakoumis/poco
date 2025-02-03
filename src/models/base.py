from pydantic import BaseModel, Field


class BaseTableOperation(BaseModel):
    """Base model for table operations."""

    target_table: str = Field(min_length=1)
