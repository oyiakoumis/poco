web: PYTHONPATH=src gunicorn -k uvicorn.workers.UvicornWorker -w 4 -b 0.0.0.0:$PORT src.api.main:app
