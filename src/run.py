"""Run the FastAPI server."""

import uvicorn

from api.config import settings

if __name__ == "__main__":
    uvicorn.run("api.main:app", host=settings.host, port=settings.port)
