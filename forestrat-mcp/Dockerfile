# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DATABASE_PATH=/app/data/multi_exchange_data_lake.duckdb
ENV LOG_LEVEL=INFO

# Create app directory and user
RUN useradd --create-home --shell /bin/bash app
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main_fixed.py .
COPY config.py .
COPY database.py .

# Create directories for data and logs
RUN mkdir -p /app/data /app/logs && \
    chown -R app:app /app

# Switch to non-root user
USER app

# Expose port for MCP server (if needed for TCP mode)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from database import DuckDBConnection; import os; db = DuckDBConnection(os.getenv('DATABASE_PATH')); print('healthy' if db.test_connection() else exit(1))"

# Default command
CMD ["python", "main_fixed.py"] 