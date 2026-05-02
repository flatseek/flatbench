FROM python:3.11-slim

WORKDIR /flatbench

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install flatseek (the search engine)
RUN pip install flatseek

# Install flatbench as package
RUN pip install flatbench

# Default command: start flatseek API server
CMD ["python", "-m", "flatseek.api_server"]
