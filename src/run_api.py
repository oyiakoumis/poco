"""Run the FastAPI server."""

import multiprocessing

import uvicorn

from config import settings

if __name__ == "__main__":
    # Calculate optimal number of workers based on CPU cores
    num_cores = multiprocessing.cpu_count()
    num_workers = (2 * num_cores) + 1

    uvicorn.run(
        "api.main:app",
        host=settings.host,
        port=settings.port,
        workers=num_workers
    )
