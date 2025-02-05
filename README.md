# Document Database Backend with Natural Language Interface

This project implements a MongoDB-based document database backend with schema validation, user isolation, and advanced querying capabilities. It features a natural language interface that allows users to interact with their data using everyday language.

## Core Components

### Natural Language Processing Pipeline

#### Query Processing
- Temporal reference resolution (e.g., "tomorrow", "next week")
- Context-aware reference resolution
- Query normalization and intent understanding

#### Collection Management
- Automatic collection identification
- Dynamic collection creation
- Schema inference from natural language

#### Document Operations
- Smart document routing
- Context-aware document identification
- Automatic data validation

### DocumentDB (src/database_manager/document_db.py)
The main database manager class that handles:
- Collection management (create, update, delete)
- Document operations (CRUD)
- Query operations with filtering, sorting, and pagination
- Aggregation operations with grouping and metrics
- Schema validation and enforcement
- User data isolation

### Schema System (src/models/schema.py)
Defines the schema validation system with support for:
- Field types:
  - STRING
  - INTEGER
  - FLOAT
  - BOOLEAN
  - DATETIME
  - SELECT (single option)
  - MULTI_SELECT (multiple options)
- Field properties:
  - Required/optional
  - Default values (including callable defaults like datetime.utcnow)
  - Options for select/multi-select fields
  - Field descriptions

## Natural Language Features

### Query Understanding
- Temporal expression resolution
- Context-aware reference resolution
- Intent classification
- Entity extraction

### Example Queries
```
"Add eggs to my list of groceries"
"Mark all my tasks for today as completed"
"What's on my grocery list?"
"Add a task to review the project tomorrow"
"Show me all tasks due this week"
```

### Smart Collection Management
- Automatic collection identification based on query context
- Dynamic collection creation with inferred schemas
- Collection purpose detection and metadata management

### Intelligent Document Routing
- Context-aware document identification
- Smart filtering and query generation
- Automatic data validation against schemas

## Implementation Details

### Agent-based Architecture
The system uses a multi-agent architecture with LangChain and LangGraph for orchestration:

#### Query Preprocessor Agent (src/agents/query_processor.py)
- Normalizes user queries
- Resolves temporal references using TemporalReferenceTool
- Resolves contextual references using conversation history
- Maintains conversation context

#### Collection Router Agent (src/agents/collection_router.py)
- Analyzes preprocessed queries
- Identifies relevant collections
- Creates new collections with inferred schemas
- Manages collection metadata

#### Document Router Agent (src/agents/document_router.py)
- Identifies specific documents
- Generates query filters
- Handles document creation decisions
- Routes operations to correct documents

#### Action Agent (src/agents/action_agent.py)
- Determines required database operations
- Validates data against schemas
- Executes operations through DatabaseOperationTool
- Formats responses for users

### Workflow Management (src/workflow/graph_manager.py)
- Orchestrates agent interactions using LangGraph
- Manages state transitions between agents
- Handles error propagation
- Ensures proper execution flow

### Tools
- TemporalReferenceTool: Resolves time-based references
- DatabaseOperationTool: Executes database operations
- ListCollectionsTool: Retrieves available collections
- ListDocumentsTool: Retrieves documents from collections
- CreateCollectionTool: Creates new collections

### Conversation Management (src/managers/conversation_manager.py)
- Maintains conversation context using proper chat message format
- Resolves implicit references
- Tracks recent interactions
- Manages user-specific state

### MongoDB Integration
- Uses motor for async operations
- Proper ObjectId handling
- Index management
- Aggregation pipeline building

### Testing
Comprehensive test suite covering:
- Collection operations
- CRUD operations
- Query operations
- Schema validation
- Error cases
- Natural language processing
- Agent interactions

## Supported Collections

### Groceries Collection
Schema:
- item (STRING, required): Name of the grocery item
- quantity (INTEGER, optional, default=1): Quantity of the item
- purchased (BOOLEAN, optional, default=false): Whether the item has been purchased

Operations:
- Create: "Add [item] to my list of groceries"
- Read: "What's on my grocery list?"
- Update: Mark items as purchased
- Delete: Remove items from list

### Tasks Collection
Schema:
- title (STRING, required): Title of the task
- description (STRING, optional): Description of the task
- due_date (DATETIME, optional): Due date of the task
- completed (BOOLEAN, optional, default=false): Whether the task is completed

Operations:
- Create: "Add a task to [description] (tomorrow/today)"
- Read: "Show me all tasks due (today/this week)"
- Update: "Mark all my tasks for today as completed"
- Delete: Remove tasks

## Current Status

### Implemented Features
- Complete agent-based architecture with LangChain/LangGraph integration
- Robust temporal reference resolution with date handling
- Context-aware query processing with chat history support
- Dynamic collection routing between groceries and tasks
- Intelligent operation type detection (create, read, update, delete)
- Schema validation and enforcement for all collections
- Proper conversation context management with chat messages
- Comprehensive logging for debugging and monitoring

### Working Functionality
- Natural language query processing with temporal understanding
- Automatic collection selection based on query context
- Creation of grocery items and tasks with proper schemas
- Temporal-based task scheduling (today, tomorrow, this week)
- Reading and filtering tasks by date ranges
- Marking tasks as completed
- Viewing grocery lists
- Conversation history tracking for context awareness

### Next Steps
1. Add support for more complex temporal queries (next month, specific dates)
2. Implement quantity parsing for grocery items
3. Add support for task priorities and categories
4. Enhance error messages with suggested corrections
5. Add support for bulk operations
6. Implement undo/redo functionality

## Environment Setup

Required environment variables:
- `DATABASE_CONNECTION_STRING`: MongoDB connection string
- `OPENAI_API_KEY`: OpenAI API key for language models
- `LOGLEVEL`: Logging level (INFO, DEBUG, etc.)

## Dependencies

Core:
- motor: Async MongoDB driver
- pydantic>=2.0.0: Data validation
- pymongo: MongoDB operations
- langchain: LLM orchestration
- langchain-openai: OpenAI integration
- langchain-community: Community tools and utilities
- langgraph: Agent workflow management
- python-dateutil: Date parsing
- pytz: Timezone handling
- rich: Console output formatting

Development:
- pytest: Testing framework
- pytest-asyncio: Async test support
- pytest-cov: Coverage reporting
- black: Code formatting
- isort: Import sorting
- mypy: Type checking
