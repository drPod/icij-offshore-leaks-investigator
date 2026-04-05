#!/usr/bin/env python3
"""Download ICIJ Offshore Leaks CSV data and Power Players list."""

import json
import os
import sys
import urllib.request
import zipfile

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CSV_DIR = os.path.join(DATA_DIR, "icij_csv")
ZIP_PATH = os.path.join(DATA_DIR, "full-oldb.LATEST.zip")
POWER_PLAYERS_PATH = os.path.join(DATA_DIR, "power_players.json")

CSV_URL = "https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip"
POWER_PLAYERS_URL = "https://offshoreleaks.icij.org/power-players.json"


def download_file(url: str, dest: str, label: str) -> None:
    """Download a file with progress reporting."""
    if os.path.exists(dest) and os.path.getsize(dest) > 1000:
        print(f"  {label}: already exists at {dest}, skipping download")
        return

    print(f"  {label}: downloading from {url}")

    req = urllib.request.Request(url, headers={"User-Agent": "ICIJ-Explorer/1.0"})
    response = urllib.request.urlopen(req)
    total = int(response.headers.get("Content-Length", 0))

    downloaded = 0
    chunk_size = 1024 * 256  # 256KB chunks

    with open(dest, "wb") as f:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = downloaded * 100 // total
                mb = downloaded / (1024 * 1024)
                total_mb = total / (1024 * 1024)
                print(f"\r    {mb:.1f}/{total_mb:.1f} MB ({pct}%)", end="", flush=True)

    print(f"\n    Done: {os.path.getsize(dest) / (1024*1024):.1f} MB")


def download_csv_data() -> str:
    """Download and extract the ICIJ CSV zip. Returns path to extracted dir."""
    os.makedirs(DATA_DIR, exist_ok=True)

    download_file(CSV_URL, ZIP_PATH, "ICIJ CSV data")

    if os.path.exists(CSV_DIR) and len(os.listdir(CSV_DIR)) >= 6:
        print(f"  CSV files already extracted to {CSV_DIR}")
        return CSV_DIR

    print(f"  Extracting to {CSV_DIR}...")
    os.makedirs(CSV_DIR, exist_ok=True)
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        zf.extractall(CSV_DIR)

    files = os.listdir(CSV_DIR)
    print(f"  Extracted {len(files)} files: {', '.join(sorted(files))}")
    return CSV_DIR


def fetch_power_players() -> list:
    """Fetch Power Players JSON list. Returns list of power player dicts."""
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(POWER_PLAYERS_PATH):
        print(f"  Power Players: already exists at {POWER_PLAYERS_PATH}")
        with open(POWER_PLAYERS_PATH) as f:
            data = json.load(f)
        print(f"  {len(data)} power players loaded from cache")
        return data

    print(f"  Power Players: fetching from {POWER_PLAYERS_URL}")
    req = urllib.request.Request(
        POWER_PLAYERS_URL, headers={"User-Agent": "ICIJ-Explorer/1.0"}
    )
    response = urllib.request.urlopen(req)
    raw = response.read().decode("utf-8")
    data = json.loads(raw)

    with open(POWER_PLAYERS_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  {len(data)} power players saved to {POWER_PLAYERS_PATH}")

    # Show a sample entry
    if data:
        sample = data[0]
        print(f"  Sample entry: {json.dumps(sample, indent=4)}")

    return data


def main():
    print("=" * 60)
    print("ICIJ Offshore Leaks Data Download")
    print("=" * 60)

    print("\n[1/2] Downloading CSV data (~70 MB)...")
    csv_dir = download_csv_data()

    print("\n[2/2] Fetching Power Players list...")
    players = fetch_power_players()

    print("\n" + "=" * 60)
    print("Download complete!")
    print(f"  CSV directory: {csv_dir}")
    print(f"  Power Players: {len(players)} entries")
    print("=" * 60)


if __name__ == "__main__":
    main()
