FROM python:3.12-slim

WORKDIR /app

# Install Jac and dependencies
RUN pip install --no-cache-dir jaclang litellm byllm

# Copy project files
COPY . .

# Download ICIJ data and build SQLite DB
RUN python scripts/download_icij.py && python scripts/ingest_icij.py

# Expose port
EXPOSE 8000

# Start Jac server on Railway's assigned port
CMD jac start main.jac --port ${PORT:-8000}
