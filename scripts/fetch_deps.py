#!/usr/bin/env python3
"""Fetch npm dependency graphs from deps.dev and merge into a single graph JSON."""

import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SEEDS = [
    "express", "react", "webpack", "next", "lodash",
    "chalk", "commander", "axios", "debug", "semver",
    "typescript", "eslint", "jest", "postcss", "minimist",
    "glob", "yargs", "inquirer", "moment", "dotenv",
]

# Hardcoded weekly download counts for popular packages (approximate, stable enough)
DOWNLOAD_COUNTS = {
    "debug": 280_000_000,
    "lodash": 250_000_000,
    "semver": 200_000_000,
    "chalk": 200_000_000,
    "supports-color": 190_000_000,
    "minimist": 150_000_000,
    "ms": 180_000_000,
    "commander": 130_000_000,
    "glob": 120_000_000,
    "yargs": 100_000_000,
    "react": 95_000_000,
    "has-flag": 160_000_000,
    "color-convert": 150_000_000,
    "color-name": 150_000_000,
    "ansi-styles": 160_000_000,
    "escape-string-regexp": 130_000_000,
    "safe-buffer": 110_000_000,
    "axios": 80_000_000,
    "express": 65_000_000,
    "typescript": 70_000_000,
    "postcss": 60_000_000,
    "eslint": 45_000_000,
    "inquirer": 40_000_000,
    "webpack": 35_000_000,
    "moment": 30_000_000,
    "jest": 25_000_000,
    "next": 20_000_000,
    "dotenv": 40_000_000,
    "inherits": 170_000_000,
    "isarray": 100_000_000,
    "readable-stream": 90_000_000,
    "string_decoder": 80_000_000,
    "util-deprecate": 80_000_000,
    "once": 90_000_000,
    "wrappy": 90_000_000,
    "mkdirp": 70_000_000,
    "path-is-absolute": 60_000_000,
    "balanced-match": 120_000_000,
    "brace-expansion": 120_000_000,
    "concat-map": 120_000_000,
    "minimatch": 110_000_000,
    "lru-cache": 100_000_000,
    "yallist": 80_000_000,
    "mime-types": 75_000_000,
    "mime-db": 75_000_000,
    "accepts": 65_000_000,
    "content-type": 60_000_000,
    "depd": 60_000_000,
    "on-finished": 60_000_000,
    "send": 55_000_000,
    "body-parser": 55_000_000,
    "cookie": 55_000_000,
    "qs": 55_000_000,
    "type-is": 55_000_000,
    "vary": 55_000_000,
    "bytes": 55_000_000,
    "raw-body": 55_000_000,
    "iconv-lite": 50_000_000,
    "safer-buffer": 50_000_000,
    "ee-first": 50_000_000,
    "statuses": 50_000_000,
    "destroy": 50_000_000,
    "etag": 50_000_000,
    "fresh": 50_000_000,
    "merge-descriptors": 50_000_000,
    "methods": 50_000_000,
    "serve-static": 50_000_000,
    "range-parser": 50_000_000,
    "parseurl": 50_000_000,
    "path-to-regexp": 50_000_000,
    "proxy-addr": 50_000_000,
    "utils-merge": 50_000_000,
    "follow-redirects": 45_000_000,
    "form-data": 40_000_000,
    "combined-stream": 40_000_000,
    "delayed-stream": 40_000_000,
    "asynckit": 40_000_000,
    "react-dom": 30_000_000,
    "loose-envify": 40_000_000,
    "js-tokens": 40_000_000,
    "object-assign": 40_000_000,
    "scheduler": 25_000_000,
    "graceful-fs": 80_000_000,
    "picomatch": 70_000_000,
    "fill-range": 65_000_000,
    "to-regex-range": 65_000_000,
    "is-number": 65_000_000,
    "braces": 65_000_000,
    "micromatch": 60_000_000,
    "anymatch": 55_000_000,
    "chokidar": 50_000_000,
    "fsevents": 45_000_000,
    "resolve": 70_000_000,
    "source-map": 60_000_000,
    "nanoid": 55_000_000,
    "picocolors": 70_000_000,
    "source-map-js": 55_000_000,
}

DEFAULT_DOWNLOADS = 1000
API_BASE = "https://api.deps.dev/v3"
DELAY = 0.2


def fetch_json(url: str, silent: bool = False) -> dict | None:
    """Fetch JSON from a URL, return None on error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "npm-supply-chain-sim/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as e:
        if not silent:
            print(f"  WARN: {url} -> {e}", file=sys.stderr)
        return None


def get_default_version(package_name: str) -> str | None:
    """Get the default (latest) version of an npm package from deps.dev."""
    encoded = urllib.parse.quote(package_name, safe="")
    url = f"{API_BASE}/systems/NPM/packages/{encoded}"
    data = fetch_json(url)
    if not data:
        return None

    versions = data.get("versions", [])
    # Find default version
    for v in versions:
        if v.get("isDefault"):
            return v["versionKey"]["version"]

    # Fallback: last non-deprecated version
    for v in reversed(versions):
        if not v.get("isDeprecated"):
            return v["versionKey"]["version"]

    # Last resort: last version in list
    if versions:
        return versions[-1]["versionKey"]["version"]

    return None


def get_dependencies(package_name: str, version: str) -> dict | None:
    """Fetch the full dependency graph for a specific package version."""
    encoded_name = urllib.parse.quote(package_name, safe="")
    encoded_ver = urllib.parse.quote(version, safe="")
    url = f"{API_BASE}/systems/NPM/packages/{encoded_name}/versions/{encoded_ver}:dependencies"
    return fetch_json(url)


def fetch_seed_graph(package_name: str) -> dict | None:
    """Fetch dep graph for a seed package, with version fallback."""
    print(f"  Fetching {package_name}...")
    version = get_default_version(package_name)
    if not version:
        print(f"  SKIP: could not find version for {package_name}", file=sys.stderr)
        return None
    time.sleep(DELAY)

    deps = get_dependencies(package_name, version)
    if deps and not deps.get("error"):
        nodes = deps.get("nodes", [])
        print(f"  OK: {package_name}@{version} -> {len(nodes)} nodes")
        return deps

    # Fallback: try to get the package versions list and try a few
    print(f"  WARN: {package_name}@{version} deps failed, trying fallback versions...")
    encoded = urllib.parse.quote(package_name, safe="")
    pkg_data = fetch_json(f"{API_BASE}/systems/NPM/packages/{encoded}")
    if not pkg_data:
        return None

    versions = pkg_data.get("versions", [])
    # Try last 5 non-deprecated versions
    candidates = [v["versionKey"]["version"] for v in reversed(versions) if not v.get("isDeprecated")][:5]
    for v in candidates:
        if v == version:
            continue
        time.sleep(DELAY)
        deps = get_dependencies(package_name, v)
        if deps and not deps.get("error"):
            nodes = deps.get("nodes", [])
            print(f"  OK: {package_name}@{v} (fallback) -> {len(nodes)} nodes")
            return deps

    print(f"  SKIP: no working version for {package_name}", file=sys.stderr)
    return None


def fetch_with_retry(url: str, max_retries: int = 3) -> dict | None:
    """Fetch JSON with retry and backoff for rate limiting."""
    for attempt in range(max_retries):
        result = fetch_json(url, silent=True)
        if result is not None:
            return result
        wait = 2 ** attempt
        time.sleep(wait)
    return None


def fetch_download_counts(package_names: list[str]) -> dict[str, int]:
    """Fetch real weekly download counts from the npm registry API."""
    counts = {}
    # Split scoped vs non-scoped
    bulk_names = [n for n in package_names if not n.startswith("@")]
    scoped_names = [n for n in package_names if n.startswith("@")]

    # Bulk fetch non-scoped in batches of 30 (shorter URLs, fewer rate limits)
    BATCH = 30
    batches = [bulk_names[i:i+BATCH] for i in range(0, len(bulk_names), BATCH)]
    print(f"  Fetching download counts: {len(bulk_names)} bulk + {len(scoped_names)} scoped...")

    for i, batch in enumerate(batches):
        joined = ",".join(batch)
        url = f"https://api.npmjs.org/downloads/point/last-week/{joined}"
        data = fetch_with_retry(url)
        if data:
            for name in batch:
                pkg_data = data.get(name)
                if pkg_data and isinstance(pkg_data, dict):
                    counts[name] = pkg_data.get("downloads", DEFAULT_DOWNLOADS)
        if (i + 1) % 5 == 0 or i == len(batches) - 1:
            print(f"    Bulk batch {i+1}/{len(batches)} ({len(counts)} counts)")
        time.sleep(1)  # 1s between batches to avoid rate limit

    # Scoped packages: fetch individually with delay
    for i, name in enumerate(scoped_names):
        encoded = urllib.parse.quote(name, safe="")
        url = f"https://api.npmjs.org/downloads/point/last-week/{encoded}"
        data = fetch_with_retry(url)
        if data and isinstance(data, dict):
            counts[name] = data.get("downloads", DEFAULT_DOWNLOADS)
        if (i + 1) % 20 == 0 or i == len(scoped_names) - 1:
            print(f"    Scoped {i+1}/{len(scoped_names)} ({len(counts)} counts)")
        time.sleep(0.5)  # 0.5s between individual fetches

    return counts


def merge_graphs(seed_results: list[dict]) -> tuple[list[dict], dict]:
    """
    Merge all dep graphs into a unified package list and graph structure.
    Returns (packages_list, graph_dict).
    """
    # all_packages: name -> {dependencies: set of names}
    all_packages: dict[str, set] = {}

    for result in seed_results:
        nodes = result.get("nodes", [])
        edges = result.get("edges", [])

        # Build index: node_index -> package_name
        idx_to_name = {}
        for i, node in enumerate(nodes):
            vk = node.get("versionKey", {})
            name = vk.get("name", "")
            if name:
                idx_to_name[i] = name
                if name not in all_packages:
                    all_packages[name] = set()

        # Process edges: fromNode depends on toNode
        for edge in edges:
            from_idx = edge.get("fromNode", -1)
            to_idx = edge.get("toNode", -1)
            from_name = idx_to_name.get(from_idx)
            to_name = idx_to_name.get(to_idx)
            if from_name and to_name and from_name != to_name:
                all_packages[from_name].add(to_name)
                # Ensure the dependency also exists as a package
                if to_name not in all_packages:
                    all_packages[to_name] = set()

    # Fetch real download counts
    all_names = sorted(all_packages.keys())
    real_counts = fetch_download_counts(all_names)

    # Build output
    packages_list = []
    for name, deps in sorted(all_packages.items()):
        downloads = real_counts.get(name, DOWNLOAD_COUNTS.get(name, DEFAULT_DOWNLOADS))
        packages_list.append({
            "name": name,
            "weekly_downloads": downloads,
            "dependencies": sorted(deps),
        })

    # Build 3d-force-graph format
    nodes = []
    links = []
    for pkg in packages_list:
        downloads = pkg["weekly_downloads"]
        val = max(1, math.log10(downloads + 1) * 2)
        nodes.append({
            "id": pkg["name"],
            "name": pkg["name"],
            "val": round(val, 2),
            "downloads": downloads,
        })
        for dep in pkg["dependencies"]:
            links.append({
                "source": pkg["name"],
                "target": dep,
            })

    graph = {"nodes": nodes, "links": links}
    return packages_list, graph


def main():
    print(f"Fetching dependency graphs for {len(SEEDS)} seed packages...\n")

    results = []
    for seed in SEEDS:
        result = fetch_seed_graph(seed)
        if result:
            results.append(result)
        time.sleep(DELAY)

    print(f"\nMerging {len(results)} graphs...")
    packages_list, graph = merge_graphs(results)

    output = {
        "packages": packages_list,
        "graph": graph,
    }

    out_path = "data/npm_graph.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nDone! Saved to {out_path}")
    print(f"  Packages: {len(packages_list)}")
    print(f"  Links: {len(graph['links'])}")

    # Show top packages by dependency count (most depended on)
    dep_counts: dict[str, int] = {}
    for pkg in packages_list:
        for dep in pkg["dependencies"]:
            dep_counts[dep] = dep_counts.get(dep, 0) + 1
    top = sorted(dep_counts.items(), key=lambda x: -x[1])[:15]
    print(f"\n  Most depended-on packages:")
    for name, count in top:
        print(f"    {name}: {count} dependents")


if __name__ == "__main__":
    main()
