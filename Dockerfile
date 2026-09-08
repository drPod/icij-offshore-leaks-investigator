FROM python:3.12-slim

WORKDIR /app

# Pin to current PyPI releases so unpinned jaclang/byllm cannot break the image.
# Do not install jac-scale: Railway used the built-in Jac API server (`GET /`
# returns "Jac API Server"). jac-scale replaces that with FastAPI catch-alls.
RUN pip install --no-cache-dir \
    jaclang==0.16.7 \
    byllm==0.6.19 \
    "litellm>=1.70,<2"

COPY . .
RUN chmod +x /app/docker-entrypoint.sh

# ICIJ CSVs + SQLite are built at first container start into the /app/data
# bind mount so image builds stay fast and rebuilds skip the ~70MB download.
EXPOSE 8000
ENV PORT=8000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
