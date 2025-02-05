from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class ChatMessage(BaseModel):
    """A chat message in the conversation history"""
    role: str = Field(description="Role of the message sender (user or assistant)")
    content: str = Field(description="Content of the message")

class ConversationContext(BaseModel):
    """Context maintained between conversations"""
    user_id: str = Field(description="ID of the user")
    conversation_history: List[ChatMessage] = Field(
        default_factory=list,
        description="List of previous chat messages"
    )
    last_collection: Optional[str] = Field(
        default=None,
        description="Name of the last collection accessed"
    )
    last_document_ids: Optional[List[str]] = Field(
        default=None,
        description="IDs of the last documents accessed"
    )

class PreprocessedQuery(BaseModel):
    """Query after preprocessing and reference resolution"""
    original_query: str = Field(description="Original user query")
    normalized_query: str = Field(description="Query with resolved references")
    temporal_references: Dict = Field(
        default_factory=dict,
        description="Resolved temporal references"
    )
    context_references: Dict = Field(
        default_factory=dict,
        description="Resolved contextual references"
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if preprocessing failed"
    )

class CollectionReference(BaseModel):
    """Reference to a collection"""
    collection_name: str = Field(description="Name of the collection")
    confidence_score: float = Field(
        description="Confidence in the collection match (0.0 to 1.0)"
    )
    schema: Dict = Field(description="Schema of the collection")
    create_new: bool = Field(
        default=False,
        description="Whether to create a new collection"
    )
    description: Optional[str] = Field(
        default=None,
        description="Description of the collection's purpose"
    )
    user_id: Optional[str] = Field(
        default=None,
        description="ID of the user who owns the collection"
    )

class DocumentReference(BaseModel):
    """Reference to one or more documents"""
    document_ids: Optional[List[str]] = Field(
        default=None,
        description="IDs of specific documents"
    )
    create_new: bool = Field(
        default=False,
        description="Whether to create new documents"
    )
    filters: Optional[Dict] = Field(
        default=None,
        description="Query filters to identify documents"
    )

class DatabaseOperation(BaseModel):
    """Database operation to execute"""
    operation_type: str = Field(
        description="Type of operation (create, read, update, delete)"
    )
    collection_name: str = Field(description="Name of the collection")
    document_ids: Optional[List[str]] = Field(
        default=None,
        description="IDs of documents to operate on"
    )
    data: Optional[Dict] = Field(
        default=None,
        description="Data for create/update operations"
    )
    filters: Optional[Dict] = Field(
        default=None,
        description="Query filters for read/update/delete operations"
    )

    def dict(self) -> Dict:
        """Convert to dictionary, excluding None values"""
        return {
            k: v for k, v in super().dict().items()
            if v is not None
        }

class AgentResponse(BaseModel):
    """Response from an agent"""
    success: bool = Field(description="Whether the operation succeeded")
    message: str = Field(description="Human-readable response message")
    error: Optional[str] = Field(
        default=None,
        description="Error message if operation failed"
    )
    data: Optional[Dict] = Field(
        default=None,
        description="Additional data from the operation"
    )
