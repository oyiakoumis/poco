"""Base models and utilities for the document store module."""

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from pydantic_core import core_schema


class PydanticUUID(UUID):
    """UUID field for Pydantic models that defaults to UUID v4."""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(UUID),
                    core_schema.chain_schema(
                        [
                            core_schema.str_schema(),
                            core_schema.no_info_plain_validator_function(cls.validate),
                        ]
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(lambda x: str(x), when_used="always"),
        )

    @classmethod
    def validate(cls, v):
        if isinstance(v, UUID):
            return v
        if isinstance(v, str):
            try:
                return UUID(v)
            except Exception:
                raise ValueError("Invalid UUID format")
        if v is None:
            # Generate a UUID v4 when no input is provided
            return uuid4()
        raise ValueError("Invalid UUID")


class BaseDocument(BaseModel):
    """Base model for all document store models."""

    id: Optional[PydanticUUID] = Field(default_factory=uuid4, alias="_id")
    user_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True, "from_attributes": True}
