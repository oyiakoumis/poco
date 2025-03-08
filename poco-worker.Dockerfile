FROM python:3.12

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

# Command to run the WhatsApp worker
CMD ["python", "src/run_whatsapp_worker.py"]
