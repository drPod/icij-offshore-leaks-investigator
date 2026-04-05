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

# Start Jac server — Railway routes to port 8000 via PORT env var
CMD ["jac", "start", "main.jac"]
