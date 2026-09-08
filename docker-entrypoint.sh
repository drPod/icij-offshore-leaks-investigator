#!/bin/sh
set -eu
cd /app

if [ ! -s /app/data/icij.db ]; then
  echo "ICIJ DB missing; downloading CSVs and ingesting into SQLite..."
  python scripts/download_icij.py
  python scripts/ingest_icij.py
else
  echo "ICIJ DB present at /app/data/icij.db; skipping download/ingest."
fi

exec jac start main.jac
