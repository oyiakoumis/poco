# POCO - AI-Powered Personal Assistant

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-green.svg)](https://fastapi.tiangolo.com)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2+-purple.svg)](https://langchain-ai.github.io/langgraph/)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://docker.com)

> **POCO** is an AI assistant that transforms WhatsApp into a powerful personal information management platform. Users interact through natural language to store, query, and manage their personal data.

## 🚀 What It Does

POCO lets you manage your personal information through WhatsApp conversations. Ask it to remember your workouts, track your ideas, manage your tasks, or store any personal data - all through natural language.

**Example Conversations:**
- "Remember that I worked out for 45 minutes today"
- "What did I do last week?"
- "Add 'Learn Python' to my goals"
- "Show me all my workout sessions from this month"

## 🏗️ Architecture

```
WhatsApp → Twilio → FastAPI → LangGraph Agent → MongoDB
                                    ↓
                              AI Tools & Azure Storage
```

## ✨ Key Features

- **Natural Language Interface**: Talk to your data like you would to a friend
- **Intelligent Context**: Understands time references ("today", "last week")
- **Flexible Data Storage**: Automatically creates and manages data structures
- **Multi-LLM Support**: Works with OpenAI, Anthropic, Azure OpenAI, and Groq
- **Media Processing**: Handles images and documents
- **Real-time Responses**: Instant WhatsApp integration

## 🛠️ Technology Stack

**Backend & AI:**
- FastAPI - Modern async web framework
- LangGraph - AI agent orchestration
- LangChain - LLM integration
- Multiple LLM providers (OpenAI, Anthropic, Azure, Groq)

**Database & Storage:**
- MongoDB - Document database
- Azure Blob Storage - File storage
- Custom document store with flexible schemas

**Infrastructure:**
- Docker containerization
- Twilio WhatsApp API
- Azure cloud services

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- MongoDB
- Twilio account
- OpenAI API key

### Setup

1. **Clone and Install**
```bash
git clone https://github.com/oyiakoumis/poco.git
cd poco
pip install -r requirements.txt
```

2. **Configure Environment**
```bash
# Set up your environment variables
export DATABASE_CONNECTION_STRING="mongodb://localhost:27017"
export OPENAI_API_KEY="your-openai-key"
export TWILIO_ACCOUNT_SID="your-twilio-sid"
export TWILIO_AUTH_TOKEN="your-twilio-token"
```

3. **Run**
```bash
python src/run_api.py
```

### Docker

```bash
docker build -t poco .
docker run -p 8000:8000 --env-file .env poco
```

## 📡 API

### Main Endpoint
```http
POST /chat/
```
Processes WhatsApp messages from Twilio webhooks.

Interactive API docs available at `http://localhost:8000/docs`

## 🧪 Development

### Project Structure
```
src/
├── agents/           # AI agent and tools
├── api/              # FastAPI application
├── database/         # Data management
└── utils/            # Utilities
```

### Code Quality
```bash
black src/ --line-length 160
pytest tests/
```

## 🔧 Configuration

Key environment variables:
- `DATABASE_CONNECTION_STRING` - MongoDB connection
- `OPENAI_API_KEY` - AI model access
- `TWILIO_ACCOUNT_SID` - WhatsApp integration
- `AZURE_STORAGE_ACCOUNT` - File storage

## 🎯 Technical Highlights

- **Async Architecture**: High-performance concurrent request handling
- **AI Tool Integration**: Custom tools for database operations and time resolution
- **Distributed Locking**: Prevents race conditions in conversations
- **Flexible Schema**: Dynamic data structure creation and evolution
- **Production Ready**: Comprehensive error handling and logging
- **Cloud Native**: Azure integration with secure credential management

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.
