FROM python:3.12-slim

WORKDIR /app

# Install system dependencies including Redis
RUN apt-get update && apt-get install -y \
    redis-server \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create cache directory
RUN mkdir -p cache

# Expose port
EXPOSE 8080

# Run the application
CMD ["python", "app.py"]
