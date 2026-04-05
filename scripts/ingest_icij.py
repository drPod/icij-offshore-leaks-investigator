#!/usr/bin/env python3
"""Ingest ICIJ Offshore Leaks CSV data into SQLite with FTS5 and Power Player tagging."""

import csv
import json
import os
import sqlite3
import sys
import time

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CSV_DIR = os.path.join(DATA_DIR, "icij_csv")
DB_PATH = os.path.join(DATA_DIR, "icij.db")
POWER_PLAYERS_PATH = os.path.join(DATA_DIR, "power_players.json")

BATCH_SIZE = 10_000

# Map ICIJ release slugs to source_id patterns in the CSV data
RELEASE_TO_SOURCE = {
    "panama-papers": ["Panama Papers"],
    "paradise-papers": [
        "Paradise Papers - Appleby",
        "Paradise Papers - Aruba corporate registry",
        "Paradise Papers - Bahamas corporate registry",
        "Paradise Papers - Barbados corporate registry",
        "Paradise Papers - Cook Islands corporate registry",
        "Paradise Papers - Lebanon corporate registry",
        "Paradise Papers - Malta corporate registry",
        "Paradise Papers - Nevis corporate registry",
        "Paradise Papers - Samoa corporate registry",
    ],
    "pandora-papers": [
        "Pandora Papers - Alemán, Cordero, Galindo & Lee (Alcogal)",
        "Pandora Papers - Alpha Consulting",
        "Pandora Papers - Asiaciti Trust",
        "Pandora Papers - CILTrust International",
        "Pandora Papers - Commence Overseas",
        "Pandora Papers - Fidelity Corporate Services",
        "Pandora Papers - Il Shin",
        "Pandora Papers - Overseas Management Company (OMC)",
        "Pandora Papers - SFM Corporate Services",
        "Pandora Papers - Trident Trust",
    ],
    "bahamas-leaks": ["Bahamas Leaks"],
    "offshore-leaks": ["Offshore Leaks"],
}

# Source ID to human-readable leak name
SOURCE_TO_LEAK = {}
for release, sources in RELEASE_TO_SOURCE.items():
    leak_name = release.replace("-", " ").title()
    for src in sources:
        SOURCE_TO_LEAK[src] = leak_name


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables, indexes, and FTS5."""
    conn.executescript(
        """
        DROP TABLE IF EXISTS nodes;
        DROP TABLE IF EXISTS relationships;
        DROP TABLE IF EXISTS power_players;
        DROP TABLE IF EXISTS sources;

        CREATE TABLE nodes (
            node_id     TEXT PRIMARY KEY,
            node_type   TEXT NOT NULL,
            name        TEXT NOT NULL DEFAULT '',
            countries   TEXT DEFAULT '',
            country_codes TEXT DEFAULT '',
            jurisdiction TEXT DEFAULT '',
            source_id   TEXT DEFAULT '',
            address     TEXT DEFAULT '',
            company_type TEXT DEFAULT '',
            incorporation_date TEXT DEFAULT '',
            status      TEXT DEFAULT ''
        );

        CREATE TABLE relationships (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id_start TEXT NOT NULL,
            node_id_end   TEXT NOT NULL,
            rel_type      TEXT NOT NULL,
            link          TEXT DEFAULT '',
            source_id     TEXT DEFAULT '',
            start_date    TEXT DEFAULT '',
            end_date      TEXT DEFAULT ''
        );

        CREATE TABLE power_players (
            node_id TEXT PRIMARY KEY,
            name    TEXT,
            title   TEXT,
            slug    TEXT,
            release TEXT
        );

        CREATE TABLE sources (
            source_id TEXT PRIMARY KEY,
            leak_name TEXT NOT NULL
        );
    """
    )
    print("  Schema created")


def create_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes after bulk loading (faster than indexing during insert)."""
    print("  Creating indexes...")
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(name);
        CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(node_type);
        CREATE INDEX IF NOT EXISTS idx_nodes_source ON nodes(source_id);
        CREATE INDEX IF NOT EXISTS idx_rel_start ON relationships(node_id_start);
        CREATE INDEX IF NOT EXISTS idx_rel_end ON relationships(node_id_end);
        CREATE INDEX IF NOT EXISTS idx_rel_type ON relationships(rel_type);
    """
    )
    print("  Indexes created")


def create_fts(conn: sqlite3.Connection) -> None:
    """Create and populate FTS5 index for name search."""
    print("  Building FTS5 index...")
    conn.executescript(
        """
        DROP TABLE IF EXISTS nodes_fts;
        CREATE VIRTUAL TABLE nodes_fts USING fts5(
            name,
            content='nodes',
            content_rowid='rowid',
            tokenize='porter unicode61'
        );
        INSERT INTO nodes_fts(rowid, name) SELECT rowid, name FROM nodes;
    """
    )
    count = conn.execute("SELECT COUNT(*) FROM nodes_fts").fetchone()[0]
    print(f"  FTS5 index built: {count:,} entries")


def ingest_node_csv(
    conn: sqlite3.Connection, csv_path: str, node_type: str
) -> int:
    """Ingest a single node CSV file. Returns row count."""
    if not os.path.exists(csv_path):
        print(f"    WARNING: {csv_path} not found, skipping")
        return 0

    count = 0
    batch = []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",")

        for row in reader:
            node_id = row.get("node_id", "").strip()
            if not node_id:
                continue

            name = row.get("name", "").strip()
            if not name and node_type == "address":
                name = row.get("address", "").strip()
            if not name:
                name = row.get("original_name", "").strip()

            batch.append(
                (
                    node_id,
                    node_type,
                    name,
                    row.get("countries", "").strip(),
                    row.get("country_codes", "").strip(),
                    row.get("jurisdiction", row.get("jurisdiction_description", "")).strip(),
                    row.get("sourceID", "").strip(),
                    row.get("address", "").strip(),
                    row.get("company_type", "").strip(),
                    row.get("incorporation_date", "").strip(),
                    row.get("status", "").strip(),
                )
            )

            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    """INSERT OR IGNORE INTO nodes
                       (node_id, node_type, name, countries, country_codes,
                        jurisdiction, source_id, address, company_type,
                        incorporation_date, status)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
                count += len(batch)
                batch = []
                print(f"\r    {count:>10,} rows...", end="", flush=True)

    if batch:
        conn.executemany(
            """INSERT OR IGNORE INTO nodes
               (node_id, node_type, name, countries, country_codes,
                jurisdiction, source_id, address, company_type,
                incorporation_date, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            batch,
        )
        count += len(batch)

    conn.commit()
    print(f"\r    {count:>10,} rows loaded")
    return count


def ingest_nodes(conn: sqlite3.Connection) -> int:
    """Ingest all 5 node CSV files."""
    total = 0
    csv_files = [
        ("nodes-entities.csv", "entity"),
        ("nodes-officers.csv", "officer"),
        ("nodes-addresses.csv", "address"),
        ("nodes-intermediaries.csv", "intermediary"),
        ("nodes-others.csv", "other"),
    ]

    for filename, node_type in csv_files:
        path = os.path.join(CSV_DIR, filename)
        print(f"  Ingesting {filename} (type={node_type})...")
        count = ingest_node_csv(conn, path, node_type)
        total += count

    return total


def ingest_relationships(conn: sqlite3.Connection) -> int:
    """Ingest relationships.csv."""
    csv_path = os.path.join(CSV_DIR, "relationships.csv")
    if not os.path.exists(csv_path):
        print(f"  WARNING: {csv_path} not found")
        return 0

    count = 0
    batch = []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",")

        for row in reader:
            start = row.get("node_id_start", "").strip()
            end = row.get("node_id_end", "").strip()
            if not start or not end:
                continue

            batch.append(
                (
                    start,
                    end,
                    row.get("rel_type", "").strip(),
                    row.get("link", "").strip(),
                    row.get("sourceID", "").strip(),
                    row.get("start_date", "").strip(),
                    row.get("end_date", "").strip(),
                )
            )

            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    """INSERT INTO relationships
                       (node_id_start, node_id_end, rel_type, link, source_id,
                        start_date, end_date)
                       VALUES (?,?,?,?,?,?,?)""",
                    batch,
                )
                count += len(batch)
                batch = []
                print(f"\r    {count:>10,} rows...", end="", flush=True)

    if batch:
        conn.executemany(
            """INSERT INTO relationships
               (node_id_start, node_id_end, rel_type, link, source_id,
                start_date, end_date)
               VALUES (?,?,?,?,?,?,?)""",
            batch,
        )
        count += len(batch)

    conn.commit()
    print(f"\r    {count:>10,} rows loaded")
    return count


def tag_power_players(conn: sqlite3.Connection) -> int:
    """Match Power Players by name against officer nodes and tag them."""
    if not os.path.exists(POWER_PLAYERS_PATH):
        print(f"  WARNING: {POWER_PLAYERS_PATH} not found, skipping Power Player tagging")
        return 0

    with open(POWER_PLAYERS_PATH) as f:
        players = json.load(f)

    tagged = 0
    for pp in players:
        name = pp.get("subtitle", "").strip()
        title = pp.get("title", "").strip()
        slug = pp.get("slug", "").strip()
        release = pp.get("release", "").strip()

        if not name:
            continue

        # Get source_id patterns for this release
        source_patterns = RELEASE_TO_SOURCE.get(release, [])

        # Try exact name match first, filtered by source
        matched_ids = []
        if source_patterns:
            placeholders = ",".join("?" for _ in source_patterns)
            rows = conn.execute(
                f"""SELECT node_id FROM nodes
                    WHERE name = ? AND node_type = 'officer'
                    AND source_id IN ({placeholders})""",
                [name] + source_patterns,
            ).fetchall()
            matched_ids = [r[0] for r in rows]

        # Fallback: LIKE match if exact match found nothing
        if not matched_ids and source_patterns:
            placeholders = ",".join("?" for _ in source_patterns)
            rows = conn.execute(
                f"""SELECT node_id FROM nodes
                    WHERE name LIKE ? AND node_type = 'officer'
                    AND source_id IN ({placeholders})
                    LIMIT 5""",
                [f"%{name}%"] + source_patterns,
            ).fetchall()
            matched_ids = [r[0] for r in rows]

        # Broader fallback: match without source filter
        if not matched_ids:
            rows = conn.execute(
                """SELECT node_id FROM nodes
                   WHERE name = ? AND node_type = 'officer'
                   LIMIT 5""",
                [name],
            ).fetchall()
            matched_ids = [r[0] for r in rows]

        for node_id in matched_ids:
            conn.execute(
                """INSERT OR IGNORE INTO power_players
                   (node_id, name, title, slug, release)
                   VALUES (?, ?, ?, ?, ?)""",
                (node_id, name, title, slug, release),
            )
            tagged += 1

    conn.commit()
    print(f"  Tagged {tagged} nodes as Power Players (from {len(players)} entries)")
    return tagged


def populate_sources(conn: sqlite3.Connection) -> None:
    """Populate the sources table mapping source_id to leak names."""
    for source_id, leak_name in SOURCE_TO_LEAK.items():
        conn.execute(
            "INSERT OR IGNORE INTO sources (source_id, leak_name) VALUES (?, ?)",
            (source_id, leak_name),
        )
    conn.commit()

    # Also discover any source_ids in the data not in our map
    unknown = conn.execute(
        """SELECT DISTINCT source_id FROM nodes
           WHERE source_id != '' AND source_id NOT IN (SELECT source_id FROM sources)"""
    ).fetchall()
    if unknown:
        print(f"  Note: {len(unknown)} unmapped source_ids found in data:")
        for row in unknown[:10]:
            print(f"    - {row[0]}")
            # Auto-map based on prefix
            sid = row[0]
            leak_name = sid.split(" - ")[0] if " - " in sid else sid
            conn.execute(
                "INSERT OR IGNORE INTO sources (source_id, leak_name) VALUES (?, ?)",
                (sid, leak_name),
            )
        conn.commit()


def main():
    print("=" * 60)
    print("ICIJ Offshore Leaks Data Ingestion")
    print("=" * 60)

    # Check CSVs exist
    if not os.path.exists(CSV_DIR):
        print(f"\nERROR: CSV directory not found at {CSV_DIR}")
        print("Run download_icij.py first.")
        sys.exit(1)

    # Remove existing DB
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"\nRemoved existing database at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    # Performance settings for bulk loading
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-512000")  # 512MB cache
    conn.execute("PRAGMA temp_store=MEMORY")

    start = time.time()

    print("\n[1/7] Creating schema...")
    create_schema(conn)

    print("\n[2/7] Ingesting nodes...")
    node_count = ingest_nodes(conn)

    print("\n[3/7] Ingesting relationships...")
    rel_count = ingest_relationships(conn)

    print("\n[4/7] Creating indexes...")
    create_indexes(conn)

    print("\n[5/7] Building FTS5 search index...")
    create_fts(conn)

    print("\n[6/7] Tagging Power Players...")
    pp_count = tag_power_players(conn)

    print("\n[7/7] Populating source mappings...")
    populate_sources(conn)

    print("\n  Running VACUUM and ANALYZE...")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("VACUUM")
    conn.execute("ANALYZE")
    conn.close()

    elapsed = time.time() - start
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)

    print("\n" + "=" * 60)
    print("Ingestion complete!")
    print(f"  Nodes:          {node_count:>12,}")
    print(f"  Relationships:  {rel_count:>12,}")
    print(f"  Power Players:  {pp_count:>12,}")
    print(f"  Database size:  {db_size:>10.1f} MB")
    print(f"  Time elapsed:   {elapsed:>10.1f} s")
    print(f"  Database path:  {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
