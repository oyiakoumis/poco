# Frontend Developer's Guide to the Chat API

## Overview

This API provides endpoints for managing chat conversations and messages, enabling real-time communication with an AI assistant.

## Base Configuration

- Base URL: `/`
- Content Type: `application/json`
- All responses are in JSON format

## Authentication

User identification is handled through the `user_id` parameter, which must be included in most requests.

## API Endpoints

### 1. Chat Operations

#### Send a Message
```http
POST /chat/
```

Sends a message to the AI assistant and receives a response.

**Request Body:**
```json
{
  "message": "Hello, can you help me?",
  "user_id": "user_123",
  "conversation_id": "uuid-string",
  "time_zone": "UTC",              // Optional, defaults to "UTC"
  "first_day_of_week": 0           // Optional, 0=Sunday, 1=Monday, defaults to 0
}
```

**Response:**
```json
{
  "message": "Hello! Yes, I'd be happy to help you.",
  "conversation_id": "uuid-string",
  "error": null                    // Present only if there's an error
}
```

### 2. Conversation Management

#### Create a New Conversation
```http
POST /conversations/
```

**Request Body:**
```json
{
  "title": "New Chat",
  "user_id": "user_123"
}
```

**Response:**
```json
{
  "id": "uuid-string",
  "title": "New Chat",
  "user_id": "user_123",
  "created_at": "2025-03-02T01:58:56.789Z",
  "updated_at": "2025-03-02T01:58:56.789Z"
}
```

#### List Conversations
```http
GET /conversations/?user_id={user_id}&skip={skip}&limit={limit}
```

**Query Parameters:**
- `user_id`: (required) User identifier
- `skip`: (optional) Number of records to skip (default: 0)
- `limit`: (optional) Maximum number of records to return (default: 50, max: 100)

**Response:**
```json
{
  "conversations": [
    {
      "id": "uuid-string",
      "title": "Chat Title",
      "user_id": "user_123",
      "created_at": "2025-03-02T01:58:56.789Z",
      "updated_at": "2025-03-02T01:58:56.789Z"
    }
  ],
  "total": 1
}
```

#### Get a Specific Conversation
```http
GET /conversations/{conversation_id}?user_id={user_id}
```

**Response:** Single conversation object

#### Update a Conversation
```http
PUT /conversations/{conversation_id}?user_id={user_id}
```

**Request Body:**
```json
{
  "title": "Updated Title"
}
```

**Response:** Updated conversation object

#### Delete a Conversation
```http
DELETE /conversations/{conversation_id}?user_id={user_id}
```

**Response:** No content (204)

### 3. Message Management

#### List Messages in a Conversation
```http
GET /conversations/{conversation_id}/messages?user_id={user_id}&skip={skip}&limit={limit}
```

**Query Parameters:**
- `user_id`: (required) User identifier
- `skip`: (optional) Number of records to skip (default: 0)
- `limit`: (optional) Maximum number of records to return (default: 100, max: 500)

**Response:**
```json
{
  "messages": [
    {
      "id": "uuid-string",
      "conversation_id": "uuid-string",
      "content": "Message content",
      "role": "user",
      "user_id": "user_123",
      "created_at": "2025-03-02T01:58:56.789Z"
    }
  ],
  "total": 1
}
```
