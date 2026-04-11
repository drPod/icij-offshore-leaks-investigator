# Jac / Jaseci Project

## Language

Jac is its own language. It compiles to Python bytecode (server), JavaScript (client), and native binaries. Do not guess syntax from training data.

- Run: `jac run <file.jac>`
- API server: `jac start <file.jac>`
- Tests: `jac test`

## MCP Server (jac-mcp)

Configured in `.mcp.json`. Provides Jac-specific tools, resources, and prompts.

### Required Workflow (from server instructions)

1. Call `understand_jac_and_jaseci` to get the knowledge map
2. Call `get_resource` for `jac://guide/pitfalls` and `jac://guide/patterns` before writing any code
3. Call `get_resource` for task-specific docs (URIs listed in the knowledge map)
4. Write code, then call `validate_jac` to verify it compiles
5. If validation fails, use `explain_error`, fix, and re-validate
6. Do NOT present code to the user until it passes validation

### Tools — Use These

| Tool | Purpose |
|---|---|
| `validate_jac` | Full type-check on code strings. Use before presenting any Jac code. |
| `check_syntax` | Parse-only syntax check (no type checking, faster). |
| `format_jac` | Format Jac code to standard style. |
| `lint_jac` | Style violations and unused symbols. Supports `auto_fix`. |
| `py_to_jac` | Compiler-backed transpile Python to Jac. |
| `jac_to_py` / `jac_to_js` | Compiler-backed transpile Jac to Python or JavaScript. |
| `graph_visualize` | Visualize graph as DOT or JSON from a code string. |
| `get_resource` | Fetch doc/guide by URI (e.g. `jac://guide/pitfalls`). |
| `understand_jac_and_jaseci` | Get the full knowledge map with resource URIs for any task. |
| `search_docs` | Keyword search across Jac docs. Returns ranked snippets with URIs. |
| `list_examples` / `get_example` | Browse and fetch example Jac code by category. |

### Tools — Skip, Use Bash Instead

- `run_jac` — Use `jac run <file.jac>` via Bash. Better for project files, real output.
- `execute_command` / `list_commands` / `get_command` — Thin subprocess wrapper around `jac <cmd>`. Just run `jac` commands directly via Bash.
- `explain_error` — Just 6 regex patterns with canned responses. Claude's own reasoning about compiler errors is better. Read the actual error message instead.
- `get_ast` — Rarely needed. Use only for debugging parser issues.

### Key Resources (via `get_resource`)

| URI | Content |
|---|---|
| `jac://guide/pitfalls` | Common mistakes AI models make with Jac syntax (WRONG vs RIGHT) |
| `jac://guide/patterns` | Idiomatic Jac patterns with complete working examples |
| `jac://guide/understand` | Knowledge map: what Jac/Jaseci are, 3 core paradigms, resource lookup |
| `jac://docs/cheatsheet` | Complete syntax reference |
| `jac://docs/osp` | Object-Spatial Programming: nodes, edges, walkers, CRUD, persistence |
| `jac://docs/foundation` | Full language specification |
| `jac://docs/byllm` | AI/LLM integration (`by llm`, `sem`, structured output, tool calling) |
| `jac://docs/jac-client` | Full-stack frontend: React/JSX client components |
| `jac://docs/jac-scale` | Deployment and scaling |

## Project Architecture

Tracelight is an investigative knowledge graph for the ICIJ Offshore Leaks database.

### Key files

- `main.jac` — Entry point, imports from services
- `services/offshore_leaks.sv.jac` — Entity node, LinkedTo edge, API functions (search, investigate, get_node_detail, get_db_stats), Investigate walker
- `db/icij_db.py` — Python SQLite query module (FTS5 search, BFS subgraph extraction, Power Player detection)
- `frontend/index.html` — 3D force-graph visualization (Three.js, vanilla JS)
- `scripts/download_icij.py` — Downloads ICIJ CSV data + Power Players JSON
- `scripts/ingest_icij.py` — Parses CSVs into SQLite with FTS5 indexes

### Data flow

1. User searches a name → `search()` → `db.icij_db.search_nodes()` → FTS5 query
2. User clicks result → `investigate()` → `db.icij_db.get_subgraph()` → BFS in SQLite → returns nodes with hop distances
3. Frontend animates graph depth-by-depth, flares Power Player nodes

### Jac ↔ Python interop

The Jac service imports `db.icij_db` (a Python module) directly. Type coercion note: Jac may pass function default parameters as strings — the Python module casts `int()` on numeric parameters defensively.
