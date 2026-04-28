FROM python:3.11-slim

WORKDIR /flatbench

# Install system dependencies for potential C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project
COPY . /flatbench

# Install Python dependencies
RUN pip install flatseek
RUN pip install --no-cache-dir -e .

# Default command: show help
CMD ["python", "-m", "flatbench", "generate", "--help"]