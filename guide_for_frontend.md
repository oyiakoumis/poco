# POCO - Mobile App Integration Guide

## Overview

POCO is a conversational data assistant that allows users to manage structured data through natural language interactions. This guide will help you understand how to integrate your mobile app with the POCO backend system.

## API Integration

POCO provides two main API areas: the Chat API for message processing and the Conversation API for managing conversation threads.

### Chat API

#### Primary Endpoint

```
POST /chat/
```

This is the main endpoint your mobile app will interact with. It accepts chat messages and returns formatted responses that your UI will need to render.

#### Request Format

```json
{
  "conversation_id": "unique-conversation-id",
  "user_id": "user-identifier",
  "message": "User's natural language message",
  "time_zone": "Asia/Dubai",
  "first_day_of_week": 0
}
```

**Parameters:**
- `conversation_id`: The ID of the conversation this message belongs to
- `user_id`: The user's unique identifier
- `message`: The user's natural language input
- `time_zone`: User's timezone for temporal references
- `first_day_of_week`: User's preference for week start (0 for Sunday, 1 for Monday)

### Conversation Management API

The Conversation API allows you to manage conversations and messages:

#### Endpoints

```
POST /conversations/                  # Create a new conversation
GET /conversations/                   # List conversations for a user
GET /conversations/{conversation_id}  # Get a specific conversation
PUT /conversations/{conversation_id}  # Update a conversation
DELETE /conversations/{conversation_id} # Delete a conversation
GET /conversations/{conversation_id}/messages # List messages in a conversation
POST /conversations/{conversation_id}/messages # Create a new message
```

#### Creating a Conversation

```json
// Request
POST /conversations/
{
  "user_id": "user-identifier",
  "title": "Conversation title"
}

// Response
{
  "id": "conversation-id",
  "title": "Conversation title",
  "user_id": "user-identifier",
  "created_at": "2025-02-26T18:30:00.000Z",
  "updated_at": "2025-02-26T18:30:00.000Z"
}
```

#### Listing Conversations

```
GET /conversations/?user_id=user-identifier&skip=0&limit=50
```

#### Creating a Message

```json
// Request
POST /conversations/{conversation_id}/messages
{
  "user_id": "user-identifier",
  "content": "User's message"
}

// Response
{
  "id": "message-id",
  "conversation_id": "conversation-id",
  "content": "User's message",
  "role": "user",
  "user_id": "user-identifier",
  "created_at": "2025-02-26T18:35:00.000Z"
}
```

### Response Format

#### Chat Response

The chat endpoint returns a streaming response with Server-Sent Events (SSE). The final response will be a JSON string containing an array of UI components:

```json
{
  "message": "[{\"type\":\"markdown\",\"content\":\"Here are your tasks:\"},{\"type\":\"table\",\"headers\":[\"Task\",\"Due Date\",\"Status\"],\"rows\":[[\"Finish design\",\"2025-03-01\",\"In Progress\"],[\"Submit proposal\",\"2025-03-15\",\"Not Started\"]]}]"
}
```

You'll need to parse the `message` string as JSON to get the array of components.

The streaming response allows you to show partial results as they become available, improving the perceived responsiveness of your app.

## UI Components

The backend will send responses using four main component types that your mobile app needs to render:

### 1. Tables

```json
{
  "type": "table",
  "headers": ["Name", "Age", "City"],
  "rows": [
    ["John Doe", 30, "New York"],
    ["Jane Smith", 25, "London"]
  ]
}
```

Implement a responsive table component that handles various data types and adjusts well to mobile screen sizes.

### 2. Charts

```json
{
  "type": "chart",
  "chart_type": "bar", // or "line", "pie"
  "labels": ["January", "February", "March"],
  "datasets": [
    {
      "label": "Sales",
      "data": [65, 59, 80]
    }
  ]
}
```

Use a mobile-friendly charting library that supports bar, line, and pie charts.

### 3. Checkboxes

```json
{
  "type": "checkbox",
  "items": [
    {"label": "Task 1", "checked": true},
    {"label": "Task 2", "checked": false}
  ]
}
```

Implement interactive checkboxes that users can toggle. When a checkbox state changes, send a new message to the API describing the change.

### 4. Markdown

```json
{
  "type": "markdown",
  "content": "# Title\nThis is a **formatted** text with _markdown_ syntax"
}
```

Use a markdown renderer that supports basic formatting (headings, bold, italic, lists, links).

## User Experience Considerations

1. **Conversation Management**: Use the conversation endpoints to create and manage conversations. Each conversation can have multiple messages.

2. **Message Threading**: Maintain conversation history in the UI and send both the `conversation_id` with each request.

3. **Streaming Responses**: Implement support for Server-Sent Events (SSE) to handle streaming responses from the chat endpoint.

4. **Loading States**: Show appropriate loading indicators while waiting for responses.

5. **Error Handling**: Implement graceful error handling for network issues or unexpected response formats.

6. **Input Interface**: Create a chat-like interface with a text input that supports natural language queries.

7. **Component Combinations**: The system often returns multiple components in a single response (e.g., a markdown introduction followed by a table). Your UI should handle these combinations elegantly.

## Example User Interactions

Users will interact with the system using natural language. Here are some examples:

- "Create a new task list with fields for task name, due date, and priority"
- "Show me all high priority tasks due this week"
- "Add a new task: Finish the mobile app design by next Friday"
- "Update the priority of the design task to urgent"
- "Show me a chart of tasks by priority"

Your interface should encourage these types of natural language interactions rather than form-based inputs.

## Technical Details

### Backend Architecture

The POCO system consists of four main components:

1. **API Layer** (`src/api/routers/`): Handles HTTP requests and responses using FastAPI
   - `chat.py`: Processes chat messages and streams responses
   - `conversation.py`: Manages conversation and message persistence

2. **Agent Layer** (`src/agents/`): Processes natural language using LLMs and executes appropriate actions
   - Uses LangGraph for orchestrating the conversation flow
   - Implements tools for data operations and temporal reference resolution

3. **Conversation Store** (`src/conversation_store/`): Manages conversation history and message persistence

4. **Data Layer** (`src/document_store/`): Manages data persistence in MongoDB with vector search capabilities

### Data Operations

Behind the scenes, the system handles:

- Creating and managing datasets (like "Tasks", "Projects", "Contacts")
- Defining fields with appropriate types (string, integer, date, select, etc.)
- Adding, updating, and querying records
- Formatting results into the UI components

Your app doesn't need to implement this logic - just send the user's natural language requests to the API and render the responses.
