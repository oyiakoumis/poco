# POCO - Mobile App Integration Guide

## Overview

POCO is a conversational data assistant that allows users to manage structured data through natural language interactions. This guide will help you understand how to integrate your mobile app with the POCO backend system.

## API Integration

### Primary Endpoint

```
POST /chat/
```

This is the main endpoint your mobile app will interact with. It accepts chat messages and returns formatted responses that your UI will need to render.

### Request Format

```json
{
  "thread_id": "unique-conversation-id",
  "user_id": "user-identifier",
  "message": "User's natural language message",
  "time_zone": "Asia/Dubai",
  "first_day_of_week": "0"
}
```

**Parameters:**
- `thread_id`: Maintain this ID for conversation continuity
- `user_id`: The user's unique identifier
- `message`: The user's natural language input
- `time_zone`: User's timezone for temporal references
- `first_day_of_week`: User's preference for week start

### Response Format

The response will be a JSON string containing an array of UI components:

```json
{
  "message": "[{\"type\":\"markdown\",\"content\":\"Here are your tasks:\"},{\"type\":\"table\",\"headers\":[\"Task\",\"Due Date\",\"Status\"],\"rows\":[[\"Finish design\",\"2025-03-01\",\"In Progress\"],[\"Submit proposal\",\"2025-03-15\",\"Not Started\"]]}]"
}
```

You'll need to parse the `message` string as JSON to get the array of components.

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

1. **Message Threading**: Maintain conversation history in the UI and send the `thread_id` with each request.

2. **Loading States**: Show appropriate loading indicators while waiting for responses.

3. **Error Handling**: Implement graceful error handling for network issues or unexpected response formats.

4. **Input Interface**: Create a chat-like interface with a text input that supports natural language queries.

5. **Component Combinations**: The system often returns multiple components in a single response (e.g., a markdown introduction followed by a table). Your UI should handle these combinations elegantly.

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

The POCO system consists of three main components:

1. **API Layer** (`src/api/routers/chat.py`): Handles HTTP requests and responses using FastAPI
2. **Agent Layer** (`src/agents/assistant.py`): Processes natural language using LLMs and executes appropriate actions
3. **Data Layer** (`src/document_store/dataset_manager.py`): Manages data persistence in MongoDB with vector search capabilities

### Data Operations

Behind the scenes, the system handles:

- Creating and managing datasets (like "Tasks", "Projects", "Contacts")
- Defining fields with appropriate types (string, integer, date, select, etc.)
- Adding, updating, and querying records
- Formatting results into the UI components

Your app doesn't need to implement this logic - just send the user's natural language requests to the API and render the responses.

## Implementation Tips

1. **Component Rendering**: Create reusable components for each response type (table, chart, checkbox, markdown).

2. **State Management**: Implement efficient state management to handle conversation history and UI updates.

3. **Responsive Design**: Ensure all components render well on various mobile screen sizes.

4. **Offline Support**: Consider implementing basic offline capabilities to improve user experience.

5. **Authentication**: Implement secure authentication to manage user sessions and protect user data.

Feel free to reach out if you have any questions about the API integration or component implementation!
