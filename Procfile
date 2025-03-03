web: OPENAI_API_KEY=$OPENAI_API_KEY DATABASE_CONNECTION_STRING=$DATABASE_CONNECTION_STRING PYTHONPATH=src gunicorn -k uvicorn.workers.UvicornWorker -w 4 -b 0.0.0.0:$PORT src.api.main:app
