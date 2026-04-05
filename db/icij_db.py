"""SQLite query interface for the ICIJ Offshore Leaks database."""

import json
import os
import sqlite3
import threading
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "icij.db")

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA cache_size=-128000")  # 128MB cache
        _local.conn.execute("PRAGMA query_only=ON")
    return _local.conn


def search_nodes(query: str, limit: int = 30) -> list:
    """FTS5 name search with prefix matching. Returns list of node dicts."""
    limit = int(limit)
    conn = _get_conn()

    # Sanitize: remove FTS5 special characters, add prefix matching
    sanitized = "".join(c for c in query if c.isalnum() or c.isspace() or c == "-")
    sanitized = sanitized.strip()
    if not sanitized:
        return []

    # Add prefix wildcard for partial matching
    terms = sanitized.split()
    fts_query = " ".join(f'"{t}"*' for t in terms if t)

    rows = conn.execute(
        """SELECT n.node_id, n.name, n.node_type, n.countries, n.country_codes,
                  n.source_id, n.jurisdiction, n.address, n.company_type,
                  pp.node_id IS NOT NULL AS is_power_player,
                  pp.title AS power_player_title,
                  pp.release AS power_player_release
           FROM nodes_fts fts
           JOIN nodes n ON n.rowid = fts.rowid
           LEFT JOIN power_players pp ON pp.node_id = n.node_id
           WHERE nodes_fts MATCH ?
           ORDER BY
               (pp.node_id IS NOT NULL) DESC,
               CASE n.node_type
                   WHEN 'officer' THEN 0
                   WHEN 'entity' THEN 1
                   WHEN 'intermediary' THEN 2
                   WHEN 'address' THEN 3
                   ELSE 4
               END,
               rank
           LIMIT ?""",
        (fts_query, limit),
    ).fetchall()

    return [
        {
            "node_id": r["node_id"],
            "name": r["name"],
            "node_type": r["node_type"],
            "countries": r["countries"],
            "country_codes": r["country_codes"],
            "source_id": r["source_id"],
            "jurisdiction": r["jurisdiction"],
            "is_power_player": bool(r["is_power_player"]),
            "power_player_title": r["power_player_title"] or "",
            "power_player_release": r["power_player_release"] or "",
        }
        for r in rows
    ]


def get_node(node_id: str) -> Optional[dict]:
    """Fetch a single node by ID with power player status."""
    conn = _get_conn()
    r = conn.execute(
        """SELECT n.*, pp.node_id IS NOT NULL AS is_power_player,
                  pp.title AS power_player_title,
                  pp.release AS power_player_release
           FROM nodes n
           LEFT JOIN power_players pp ON pp.node_id = n.node_id
           WHERE n.node_id = ?""",
        (node_id,),
    ).fetchone()

    if not r:
        return None

    return {
        "node_id": r["node_id"],
        "name": r["name"],
        "node_type": r["node_type"],
        "countries": r["countries"],
        "country_codes": r["country_codes"],
        "source_id": r["source_id"],
        "jurisdiction": r["jurisdiction"],
        "address": r["address"],
        "company_type": r["company_type"],
        "incorporation_date": r["incorporation_date"],
        "status": r["status"],
        "is_power_player": bool(r["is_power_player"]),
        "power_player_title": r["power_player_title"] or "",
        "power_player_release": r["power_player_release"] or "",
    }


def _batch_query(conn, sql_template: str, ids: list, extra_params: list = None) -> list:
    """Execute a query with batched IN clause (SQLite 999 variable limit)."""
    results = []
    batch_size = 900  # Leave room for extra params
    for i in range(0, len(ids), batch_size):
        batch = ids[i : i + batch_size]
        placeholders = ",".join("?" for _ in batch)
        sql = sql_template.replace("__PLACEHOLDERS__", placeholders)
        params = (extra_params or []) + batch
        results.extend(conn.execute(sql, params).fetchall())
    return results


def get_subgraph(
    center_node_id: str,
    max_depth: int = 3,
    max_nodes: int = 500,
) -> dict:
    max_depth = int(max_depth)
    max_nodes = int(max_nodes)
    """BFS from center node, returning nodes with hop distance and edges."""
    conn = _get_conn()

    # Verify center node exists
    center = get_node(center_node_id)
    if not center:
        return {
            "nodes": [],
            "edges": [],
            "power_players_found": [],
            "truncated": False,
            "stats": {"total_nodes": 0, "total_edges": 0, "max_depth_reached": 0},
        }

    visited = set()
    seen_names = set()  # Deduplicate same person appearing as multiple records
    all_nodes = []
    all_edges = []
    all_edge_keys = set()
    power_players_found = []
    truncated = False
    max_depth_reached = 0

    # Add center node at hop 0
    center["hop"] = 0
    all_nodes.append(center)
    visited.add(center_node_id)
    seen_names.add((center["name"].lower(), center["node_type"]))
    if center["is_power_player"]:
        power_players_found.append(
            {
                "node_id": center_node_id,
                "name": center["name"],
                "title": center["power_player_title"],
                "release": center["power_player_release"],
                "hop": 0,
            }
        )

    frontier = [center_node_id]

    for depth in range(1, max_depth + 1):
        if not frontier:
            break
        if len(visited) >= max_nodes:
            truncated = True
            break

        # Find all neighbors of the frontier, capping fan-out per node
        # so one super-connected intermediary/address doesn't flood the graph
        MAX_FANOUT = 15

        neighbor_ids = set()
        # Track how many new neighbors each frontier node contributes
        fanout_count = {fid: 0 for fid in frontier}

        edge_rows = _batch_query(
            conn,
            """SELECT node_id_start, node_id_end, rel_type, link, source_id
               FROM relationships
               WHERE node_id_start IN (__PLACEHOLDERS__)""",
            frontier,
        )
        edge_rows += _batch_query(
            conn,
            """SELECT node_id_start, node_id_end, rel_type, link, source_id
               FROM relationships
               WHERE node_id_end IN (__PLACEHOLDERS__)""",
            frontier,
        )

        for er in edge_rows:
            start, end = er["node_id_start"], er["node_id_end"]
            edge_key = (start, end, er["rel_type"])

            # Skip self-referencing and deduplication edges for cleaner graphs
            if start == end:
                continue
            rel = er["rel_type"]
            if rel in ("same_name_as", "similar", "same_company_as", "same_as",
                       "same_id_as", "similar_company_as", "probably_same_officer_as",
                       "same_address_as", "same_intermediary_as"):
                continue

            # Determine which side is the frontier node and which is the neighbor
            if start in fanout_count:
                frontier_node, other = start, end
            else:
                frontier_node, other = end, start

            # Cap fan-out: skip if this frontier node already contributed enough
            if other not in visited and other not in neighbor_ids:
                if fanout_count.get(frontier_node, 0) >= MAX_FANOUT:
                    continue
                fanout_count[frontier_node] = fanout_count.get(frontier_node, 0) + 1

            if edge_key not in all_edge_keys:
                all_edge_keys.add(edge_key)
                all_edges.append(
                    {
                        "source": start,
                        "target": end,
                        "rel_type": rel,
                        "link": er["link"],
                        "source_id": er["source_id"],
                    }
                )

            # Collect new neighbor IDs
            if other not in visited:
                neighbor_ids.add(other)

        # Apply node budget
        remaining = max_nodes - len(visited)
        if len(neighbor_ids) > remaining:
            # Check if any are power players — always include those
            pp_ids = set()
            if neighbor_ids:
                pp_rows = _batch_query(
                    conn,
                    "SELECT node_id FROM power_players WHERE node_id IN (__PLACEHOLDERS__)",
                    list(neighbor_ids),
                )
                pp_ids = {r["node_id"] for r in pp_rows}

            # Include power players first, then fill remaining
            priority = list(pp_ids)
            others = [nid for nid in neighbor_ids if nid not in pp_ids]
            selected = priority + others[: remaining - len(priority)]
            neighbor_ids = set(selected)
            truncated = True

        if not neighbor_ids:
            break

        # Fetch node details for new neighbors
        new_nodes = _batch_query(
            conn,
            """SELECT n.*, pp.node_id IS NOT NULL AS is_power_player,
                      pp.title AS power_player_title,
                      pp.release AS power_player_release
               FROM nodes n
               LEFT JOIN power_players pp ON pp.node_id = n.node_id
               WHERE n.node_id IN (__PLACEHOLDERS__)""",
            list(neighbor_ids),
        )

        next_frontier = []
        for r in new_nodes:
            nid = r["node_id"]
            if nid in visited:
                continue
            # Skip duplicate names (same person, different ICIJ record)
            name_key = (r["name"].lower(), r["node_type"])
            if name_key in seen_names:
                visited.add(nid)  # Mark visited so edges don't point to missing nodes
                continue
            seen_names.add(name_key)
            visited.add(nid)
            node = {
                "node_id": nid,
                "name": r["name"],
                "node_type": r["node_type"],
                "countries": r["countries"],
                "country_codes": r["country_codes"],
                "source_id": r["source_id"],
                "jurisdiction": r["jurisdiction"] or "",
                "address": r["address"] or "",
                "company_type": r["company_type"] or "",
                "is_power_player": bool(r["is_power_player"]),
                "power_player_title": r["power_player_title"] or "",
                "power_player_release": r["power_player_release"] or "",
                "hop": depth,
            }
            all_nodes.append(node)
            next_frontier.append(nid)

            if node["is_power_player"]:
                power_players_found.append(
                    {
                        "node_id": nid,
                        "name": node["name"],
                        "title": node["power_player_title"],
                        "release": node["power_player_release"],
                        "hop": depth,
                    }
                )

        frontier = next_frontier
        max_depth_reached = depth

    # Filter edges to only include those between nodes actually in the result
    node_id_set = {n["node_id"] for n in all_nodes}
    all_edges = [
        e
        for e in all_edges
        if e["source"] in node_id_set and e["target"] in node_id_set
    ]

    return {
        "nodes": all_nodes,
        "edges": all_edges,
        "power_players_found": power_players_found,
        "truncated": truncated,
        "stats": {
            "total_nodes": len(all_nodes),
            "total_edges": len(all_edges),
            "max_depth_reached": max_depth_reached,
        },
    }


def get_node_detail(node_id: str) -> Optional[dict]:
    """Fetch full node details with immediate connections and ICIJ URL."""
    conn = _get_conn()
    node = get_node(node_id)
    if not node:
        return None

    # Get immediate connections
    connections = conn.execute(
        """SELECT r.rel_type, r.link, r.source_id, r.start_date, r.end_date,
                  n.node_id, n.name, n.node_type
           FROM relationships r
           JOIN nodes n ON n.node_id = CASE
               WHEN r.node_id_start = ? THEN r.node_id_end
               ELSE r.node_id_start
           END
           WHERE (r.node_id_start = ? OR r.node_id_end = ?)
           AND r.rel_type NOT IN ('same_name_as', 'similar', 'same_company_as',
                                   'same_as', 'same_id_as')
           LIMIT 100""",
        (node_id, node_id, node_id),
    ).fetchall()

    node["connections"] = [
        {
            "rel_type": c["rel_type"],
            "link": c["link"],
            "source_id": c["source_id"],
            "start_date": c["start_date"] or "",
            "end_date": c["end_date"] or "",
            "node_id": c["node_id"],
            "name": c["name"],
            "node_type": c["node_type"],
        }
        for c in connections
    ]

    node["icij_url"] = f"https://offshoreleaks.icij.org/nodes/{node_id}"
    node["leak_name"] = get_leak_name(node["source_id"])

    return node


def get_leak_name(source_id: str) -> str:
    """Map source_id to human-readable leak name."""
    if not source_id:
        return "Unknown"
    conn = _get_conn()
    r = conn.execute(
        "SELECT leak_name FROM sources WHERE source_id = ?", (source_id,)
    ).fetchone()
    if r:
        return r["leak_name"]
    # Fallback: extract from prefix
    return source_id.split(" - ")[0] if " - " in source_id else source_id


def get_node_url(node_id: str) -> str:
    """Construct ICIJ Offshore Leaks URL for a node."""
    return f"https://offshoreleaks.icij.org/nodes/{node_id}"


def get_stats() -> dict:
    """Return database statistics."""
    conn = _get_conn()

    total = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]

    type_counts = {}
    for row in conn.execute(
        "SELECT node_type, COUNT(*) as cnt FROM nodes GROUP BY node_type"
    ):
        type_counts[row["node_type"]] = row["cnt"]

    rel_count = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0]
    pp_count = conn.execute("SELECT COUNT(*) FROM power_players").fetchone()[0]

    leaks = [
        row[0]
        for row in conn.execute("SELECT DISTINCT leak_name FROM sources ORDER BY leak_name")
    ]

    return {
        "total_nodes": total,
        "total_entities": type_counts.get("entity", 0),
        "total_officers": type_counts.get("officer", 0),
        "total_addresses": type_counts.get("address", 0),
        "total_intermediaries": type_counts.get("intermediary", 0),
        "total_others": type_counts.get("other", 0),
        "total_relationships": rel_count,
        "total_power_players": pp_count,
        "leaks": leaks,
    }


# ── Power Player Grid ────────────────────────────

_pp_json_cache = None
_pp_connectivity_cache = None

def _load_pp_json():
    global _pp_json_cache
    if _pp_json_cache is None:
        pp_path = os.path.join(os.path.dirname(__file__), "..", "data", "power_players.json")
        with open(pp_path) as f:
            _pp_json_cache = {p["slug"]: p for p in json.load(f)}
    return _pp_json_cache


def _compute_pp_connectivity(pps: list) -> list:
    """Pre-compute which PP pairs are connected. Cached after first call."""
    global _pp_connectivity_cache
    if _pp_connectivity_cache is not None:
        return _pp_connectivity_cache

    # Check for cached file first
    cache_path = os.path.join(os.path.dirname(__file__), "..", "data", "pp_connections.json")
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            _pp_connectivity_cache = json.load(f)
        return _pp_connectivity_cache

    # Compute all pairs
    connected = []
    for i in range(len(pps)):
        for j in range(i + 1, len(pps)):
            r = find_path(pps[i]["node_ids"][0], pps[j]["node_ids"][0], max_depth=8)
            if r["found"]:
                connected.append({
                    "slug_a": pps[i]["slug"],
                    "slug_b": pps[j]["slug"],
                    "hops": r["hops"],
                })

    # Cache to file for next time
    with open(cache_path, "w") as f:
        json.dump(connected, f)

    _pp_connectivity_cache = connected
    return connected


def get_power_players() -> list:
    """Return all Power Players that have DB entries, with image URLs from JSON."""
    conn = _get_conn()
    pp_json = _load_pp_json()

    rows = conn.execute(
        """SELECT slug, GROUP_CONCAT(node_id) as node_ids, name, title, release
           FROM power_players GROUP BY slug ORDER BY name"""
    ).fetchall()

    players = []
    for r in rows:
        slug = r["slug"]
        json_entry = pp_json.get(slug, {})
        players.append({
            "slug": slug,
            "name": r["name"],
            "title": r["title"],
            "release": r["release"],
            "image": json_entry.get("image", ""),
            "node_ids": r["node_ids"].split(","),
            "icij_link": json_entry.get("link", f"/power-players/{slug}"),
        })

    # Compute connectivity (cached after first call)
    connected_pairs = _compute_pp_connectivity(players)

    # Mark which PPs have connections and build adjacency
    connectable_slugs = set()
    for pair in connected_pairs:
        connectable_slugs.add(pair["slug_a"])
        connectable_slugs.add(pair["slug_b"])

    for p in players:
        p["has_connections"] = p["slug"] in connectable_slugs

    return {
        "players": players,
        "connected_pairs": connected_pairs,
        "connectable_count": len(connectable_slugs),
    }


# ── Pathfinding (Bidirectional BFS) ──────────────

EXCLUDED_RELS = frozenset({
    "same_name_as", "similar", "same_company_as", "same_as",
    "same_id_as", "similar_company_as", "probably_same_officer_as",
    "same_address_as", "same_intermediary_as",
})

PATH_MAX_FANOUT = 20


def find_path(start_id: str, end_id: str, max_depth: int = 6) -> dict:
    """Bidirectional BFS between two node IDs. Returns shortest path."""
    max_depth = int(max_depth)
    start_id = str(start_id)
    end_id = str(end_id)
    conn = _get_conn()

    if start_id == end_id:
        node = get_node(start_id)
        if node:
            node["hop"] = 0
            return {"found": True, "path_nodes": [node], "path_edges": [], "hops": 0}
        return {"found": False, "path_nodes": [], "path_edges": [], "hops": 0}

    # Forward BFS state (from start)
    fwd_visited = {start_id: None}
    fwd_frontier = [start_id]
    fwd_edge_map = {}  # node_id -> (parent_id, rel_type, link, source_id)

    # Backward BFS state (from end)
    bwd_visited = {end_id: None}
    bwd_frontier = [end_id]
    bwd_edge_map = {}

    meeting_node = None

    for depth in range(1, max_depth + 1):
        if not fwd_frontier and not bwd_frontier:
            break

        # Expand the smaller frontier (or whichever is non-empty)
        if not fwd_frontier:
            expanding = "backward"
        elif not bwd_frontier:
            expanding = "forward"
        elif len(fwd_frontier) <= len(bwd_frontier):
            expanding = "forward"
        else:
            expanding = "backward"

        if expanding == "forward":
            frontier = fwd_frontier
            visited = fwd_visited
            edge_map = fwd_edge_map
            other_visited = bwd_visited
        else:
            frontier = bwd_frontier
            visited = bwd_visited
            edge_map = bwd_edge_map
            other_visited = fwd_visited

        # Get all edges from frontier
        edge_rows = _batch_query(
            conn,
            """SELECT node_id_start, node_id_end, rel_type, link, source_id
               FROM relationships
               WHERE node_id_start IN (__PLACEHOLDERS__)""",
            frontier,
        )
        edge_rows += _batch_query(
            conn,
            """SELECT node_id_start, node_id_end, rel_type, link, source_id
               FROM relationships
               WHERE node_id_end IN (__PLACEHOLDERS__)""",
            frontier,
        )

        fanout_count = {fid: 0 for fid in frontier}
        next_frontier = []

        for er in edge_rows:
            start, end = er["node_id_start"], er["node_id_end"]
            if start == end:
                continue
            rel = er["rel_type"]
            if rel in EXCLUDED_RELS:
                continue

            # Determine frontier node vs neighbor
            frontier_set = set(frontier)
            if start in frontier_set:
                frontier_node, other = start, end
            else:
                frontier_node, other = end, start

            if other in visited:
                continue

            # Cap fanout
            if fanout_count.get(frontier_node, 0) >= PATH_MAX_FANOUT:
                continue
            fanout_count[frontier_node] = fanout_count.get(frontier_node, 0) + 1

            visited[other] = frontier_node
            edge_map[other] = (frontier_node, rel, er["link"], er["source_id"])
            next_frontier.append(other)

            # Check: did we meet the other side?
            if other in other_visited:
                meeting_node = other
                break

        if meeting_node:
            break

        if expanding == "forward":
            fwd_frontier = next_frontier
        else:
            bwd_frontier = next_frontier

    if not meeting_node:
        return {"found": False, "path_nodes": [], "path_edges": [], "hops": 0}

    # Reconstruct path: start -> meeting_node
    path_fwd_ids = []
    edges_fwd = []
    node = meeting_node
    while fwd_visited[node] is not None:
        parent, rel, link, sid = fwd_edge_map[node]
        path_fwd_ids.append(node)
        edges_fwd.append({"source": parent, "target": node, "rel_type": rel, "link": link, "source_id": sid})
        node = parent
    path_fwd_ids.append(start_id)
    path_fwd_ids.reverse()
    edges_fwd.reverse()

    # Reconstruct path: meeting_node -> end
    path_bwd_ids = []
    edges_bwd = []
    node = meeting_node
    while bwd_visited[node] is not None:
        parent, rel, link, sid = bwd_edge_map[node]
        path_bwd_ids.append(node)
        edges_bwd.append({"source": node, "target": parent, "rel_type": rel, "link": link, "source_id": sid})
        node = parent
    path_bwd_ids.append(end_id)

    # Merge (meeting_node appears in both halves)
    full_path_ids = path_fwd_ids + path_bwd_ids[1:]
    full_edges = edges_fwd + edges_bwd

    # Fetch node details
    path_nodes = []
    for i, nid in enumerate(full_path_ids):
        node = get_node(nid)
        if node:
            node["hop"] = i
            path_nodes.append(node)

    return {
        "found": True,
        "path_nodes": path_nodes,
        "path_edges": full_edges,
        "hops": len(full_path_ids) - 1,
        "meeting_node": meeting_node,
        "start_id": start_id,
        "end_id": end_id,
    }


def find_connections(node_ids: list, max_depth: int = 6) -> dict:
    """Find paths between all pairs of node IDs, merge into one subgraph."""
    max_depth = int(max_depth)
    node_ids = [str(nid) for nid in node_ids]

    all_nodes = {}  # node_id -> node dict
    all_edges = []
    all_edge_keys = set()
    paths_found = []
    paths_not_found = []

    # All unique pairs
    for i in range(len(node_ids)):
        for j in range(i + 1, len(node_ids)):
            result = find_path(node_ids[i], node_ids[j], max_depth=max_depth)
            if result["found"]:
                paths_found.append({
                    "start_id": node_ids[i],
                    "end_id": node_ids[j],
                    "hops": result["hops"],
                    "path_node_ids": [n["node_id"] for n in result["path_nodes"]],
                })
                for node in result["path_nodes"]:
                    all_nodes[node["node_id"]] = node
                for edge in result["path_edges"]:
                    ekey = (edge["source"], edge["target"], edge["rel_type"])
                    if ekey not in all_edge_keys:
                        all_edge_keys.add(ekey)
                        all_edges.append(edge)
            else:
                paths_not_found.append({"start_id": node_ids[i], "end_id": node_ids[j]})

    result = {
        "nodes": list(all_nodes.values()),
        "edges": all_edges,
        "paths_found": paths_found,
        "paths_not_found": paths_not_found,
        "stats": {
            "total_nodes": len(all_nodes),
            "total_edges": len(all_edges),
            "pairs_searched": len(paths_found) + len(paths_not_found),
            "pairs_connected": len(paths_found),
        },
    }

    # Expand the graph — add neighbors of path nodes for visual density
    if paths_found:
        result = expand_subgraph(result, max_extra=6)

    return result


# ── LLM Summary Cache ────────────────────────────

_SUMMARY_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "summary_cache.json")

def get_cached_summary(start_id: str, end_id: str) -> str:
    """Return cached LLM summary for a pair, or empty string if not cached."""
    key = f"{start_id}:{end_id}"
    alt_key = f"{end_id}:{start_id}"
    try:
        if os.path.exists(_SUMMARY_CACHE_PATH):
            with open(_SUMMARY_CACHE_PATH) as f:
                cache = json.load(f)
            return cache.get(key, cache.get(alt_key, ""))
    except Exception:
        pass
    return ""


def save_cached_summary(start_id: str, end_id: str, summary: str) -> None:
    """Persist an LLM summary so we never pay for the same call twice."""
    key = f"{start_id}:{end_id}"
    cache = {}
    try:
        if os.path.exists(_SUMMARY_CACHE_PATH):
            with open(_SUMMARY_CACHE_PATH) as f:
                cache = json.load(f)
    except Exception:
        pass
    cache[key] = summary
    with open(_SUMMARY_CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def get_neighbor_options(node_id: str) -> list:
    """Get all meaningful neighbors of a node with metadata for agent decision-making."""
    conn = _get_conn()
    node_id = str(node_id)

    rows = conn.execute(
        """SELECT
               CASE WHEN r.node_id_start = ? THEN r.node_id_end ELSE r.node_id_start END as neighbor_id,
               r.rel_type,
               n.name, n.node_type, n.countries, n.jurisdiction,
               pp.node_id IS NOT NULL as is_pp,
               pp.title as pp_title
           FROM relationships r
           JOIN nodes n ON n.node_id = CASE WHEN r.node_id_start = ? THEN r.node_id_end ELSE r.node_id_start END
           LEFT JOIN power_players pp ON pp.node_id = n.node_id
           WHERE (r.node_id_start = ? OR r.node_id_end = ?)
           AND r.rel_type NOT IN ('same_name_as','similar','same_company_as','same_as',
                                   'same_id_as','similar_company_as','probably_same_officer_as',
                                   'same_address_as','same_intermediary_as')
           AND r.node_id_start != r.node_id_end
           LIMIT 50""",
        (node_id, node_id, node_id, node_id),
    ).fetchall()

    return [
        {
            "node_id": r["neighbor_id"],
            "name": r["name"],
            "node_type": r["node_type"],
            "rel_type": r["rel_type"],
            "countries": r["countries"] or "",
            "jurisdiction": r["jurisdiction"] or "",
            "is_power_player": bool(r["is_pp"]),
            "pp_title": r["pp_title"] or "",
        }
        for r in rows
    ]


def generate_investigation_log(path_result: dict) -> list:
    """Generate a step-by-step decision log for a path, showing what the agent
    considered and why it chose each direction."""
    log = []

    for path_info in path_result.get("paths_found", []):
        path_ids = path_info.get("path_node_ids", [])
        if len(path_ids) < 2:
            continue

        # Get full node info for path
        path_nodes_map = {n["node_id"]: n for n in path_result["nodes"] if n["node_id"] in path_ids}

        start = path_nodes_map.get(path_ids[0], {})
        end = path_nodes_map.get(path_ids[-1], {})

        log.append({
            "type": "start",
            "icon": "search",
            "text": f"Starting investigation from {start.get('name', '?')}",
            "detail": f"{start.get('power_player_title') or start.get('node_type', '')} — searching for path to {end.get('name', '?')}",
        })

        for i in range(len(path_ids) - 1):
            current_id = path_ids[i]
            next_id = path_ids[i + 1]
            current = path_nodes_map.get(current_id, {})
            chosen = path_nodes_map.get(next_id, {})

            # Get ALL neighbors of current node (not just the path)
            all_neighbors = get_neighbor_options(current_id)
            other_options = [n for n in all_neighbors
                            if n["node_id"] != next_id and n["node_id"] not in path_ids[:i]]

            # Build the reason for choosing this direction
            chosen_type = chosen.get("node_type", "")
            if chosen.get("is_power_player"):
                reason = f"Power Player detected — {chosen.get('power_player_title', '')}"
                icon = "star"
                log_type = "discovery"
            elif chosen_type == "intermediary":
                reason = "Intermediary firms connect many offshore entities"
                icon = "arrow"
                log_type = "explore"
            elif chosen_type == "entity":
                jurisdiction = chosen.get("jurisdiction") or chosen.get("countries", "")
                reason = f"Shell company registered in {jurisdiction}" if jurisdiction else "Following corporate link"
                icon = "arrow"
                log_type = "explore"
            elif chosen_type == "address":
                reason = "Shared registered address — often links unrelated entities"
                icon = "arrow"
                log_type = "explore"
            else:
                reason = "Following connection"
                icon = "arrow"
                log_type = "explore"

            text = f"Exploring {chosen.get('name', '?')}"

            # Note what was skipped
            skipped_count = len(other_options)
            skip_detail = ""
            if skipped_count > 0:
                skip_names = [n["name"] for n in other_options[:3]]
                skip_detail = f"Passed on {skipped_count} other connection{'s' if skipped_count != 1 else ''}"
                if skip_names:
                    skip_detail += f": {', '.join(n[:25] for n in skip_names)}"
                    if skipped_count > 3:
                        skip_detail += f" +{skipped_count - 3} more"

            log.append({
                "type": log_type,
                "icon": icon,
                "text": text,
                "detail": reason,
                "skip": skip_detail,
                "depth": i + 1,
            })

        # Final entry
        log.append({
            "type": "found",
            "icon": "link",
            "text": f"Connection established: {start.get('name', '?')} to {end.get('name', '?')}",
            "detail": f"{path_info['hops']} hops through {len([n for n in path_nodes_map.values() if n.get('node_type') == 'entity'])} entities",
        })

    return log


def expand_subgraph(result: dict, expand_depth: int = 1, max_extra: int = 8) -> dict:
    """Expand a path result by adding neighbors of each path node.
    Turns a thin chain into a rich network."""
    conn = _get_conn()
    existing_ids = {n["node_id"] for n in result["nodes"]}
    extra_nodes = {}
    extra_edges = []
    extra_edge_keys = {(e["source"], e["target"], e["rel_type"]) for e in result["edges"]}

    for node in result["nodes"]:
        nid = node["node_id"]
        # Get neighbors not already in the graph
        neighbors = get_neighbor_options(nid)
        added = 0
        for nb in neighbors:
            if nb["node_id"] in existing_ids or nb["node_id"] in extra_nodes:
                continue
            if added >= max_extra:
                break
            # Fetch full node details
            full_node = get_node(nb["node_id"])
            if not full_node:
                continue
            full_node["hop"] = node.get("hop", 0) + 1
            extra_nodes[nb["node_id"]] = full_node
            added += 1

            # Add the edge
            ekey = (nid, nb["node_id"], nb["rel_type"])
            ekey_rev = (nb["node_id"], nid, nb["rel_type"])
            if ekey not in extra_edge_keys and ekey_rev not in extra_edge_keys:
                extra_edge_keys.add(ekey)
                extra_edges.append({
                    "source": nid,
                    "target": nb["node_id"],
                    "rel_type": nb["rel_type"],
                    "link": "",
                    "source_id": "",
                })

    result["nodes"] = result["nodes"] + list(extra_nodes.values())
    result["edges"] = result["edges"] + extra_edges
    result["stats"]["total_nodes"] = len(result["nodes"])
    result["stats"]["total_edges"] = len(result["edges"])
    return result


def build_path_description(path_nodes: list, path_edges: list) -> str:
    """Build a structured text description of a path for LLM consumption."""
    if not path_nodes:
        return ""

    parts = []
    for i, node in enumerate(path_nodes):
        jurisdiction = node.get("jurisdiction", "") or node.get("countries", "")
        desc = f"{node['name']} ({node['node_type']}"
        if jurisdiction:
            desc += f", {jurisdiction}"
        desc += ")"

        if i == 0:
            parts.append(desc)
        elif i < len(path_edges) + 1:
            edge = path_edges[i - 1]
            rel = edge["rel_type"].replace("_", " ")
            parts.append(f"--({rel})--> {desc}")

    return " ".join(parts)
