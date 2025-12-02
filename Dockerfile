FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir setuptools

# Copy project files
COPY . .

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Make entrypoint script executable
RUN chmod +x koyeb_entrypoint.py

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.path.insert(0, '/app'); from koyeb_entrypoint import health_check; exit(0 if health_check() else 1)" || exit 1

# Default command (can be overridden in Koyeb)
CMD ["python", "koyeb_entrypoint.py"]

