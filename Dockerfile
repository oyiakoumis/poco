ARG PLATFORM=linux/amd64
FROM --platform=${PLATFORM} python:3.12

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1


# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY src/ ./src/

# Expose port
EXPOSE 8000

# Create a startup script for dynamic worker calculation
RUN echo '#!/bin/bash\n\
CORES=$(nproc)\n\
WORKERS=$((2 * CORES + 1))\n\
echo "Starting with $WORKERS workers ($CORES CPU cores detected)"\n\
exec gunicorn -k uvicorn.workers.UvicornWorker -w $WORKERS -b 0.0.0.0:8000 src.api.main:app\n\
' > /app/start.sh && chmod +x /app/start.sh

# Command to run the application with dynamic worker calculation
CMD ["/app/start.sh"]
