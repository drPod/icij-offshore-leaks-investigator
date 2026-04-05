# ICIJ Offshore Leaks Investigator

Investigative knowledge graph for the ICIJ Offshore Leaks database. Search a name, watch walkers traverse through shell companies, intermediaries, and shared addresses. When the traversal hits a politically exposed person, the node flares. Every edge is traceable to a specific ICIJ record.

## Architecture

- **Backend**: Jac API server with walker-based graph traversal
- **Data**: SQLite (2M+ nodes, 3.3M edges from ICIJ Offshore Leaks CSVs)
- **Frontend**: 3D force-directed graph (Three.js) with depth-based cascade animation
- **Query layer**: Python module (`db/icij_db.py`) for FTS5 search and BFS subgraph extraction

## Setup

```bash
# 1. Download ICIJ data (~70MB)
python scripts/download_icij.py

# 2. Ingest into SQLite (~50s)
python scripts/ingest_icij.py

# 3. Start API server
jac start main.jac

# 4. Open frontend
open frontend/index.html
```

## API Endpoints

| Endpoint | Method | Body | Description |
|---|---|---|---|
| `/function/search` | POST | `{query, limit?}` | FTS5 name search across 2M+ nodes |
| `/function/investigate` | POST | `{node_id, max_depth?, max_nodes?}` | BFS subgraph with Power Player detection |
| `/function/get_node_detail` | POST | `{node_id}` | Full node details + immediate connections |
| `/function/get_db_stats` | POST | `{}` | Database statistics |

## Data Sources

- [ICIJ Offshore Leaks Database](https://offshoreleaks.icij.org/) — Panama Papers, Paradise Papers, Pandora Papers, Bahamas Leaks, Offshore Leaks
- [ICIJ Power Players](https://offshoreleaks.icij.org/power-players) — 181 politically exposed persons
