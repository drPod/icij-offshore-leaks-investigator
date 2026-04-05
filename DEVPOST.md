## Inspiration

The Panama Papers, Paradise Papers, and Pandora Papers exposed how the world's most powerful people hide wealth through offshore shell companies — but the raw ICIJ database is 2 million nodes and 3.3 million relationships. Nobody can make sense of that by scrolling through spreadsheets. We wanted to build a tool that makes these hidden financial networks *legible and dramatic* — where you pick two politically exposed persons and watch an AI agent trace the offshore connections between them in real time.

## What it does

You're presented with a grid of 94 Power Players — heads of state, prime ministers, intelligence chiefs, and their associates — sourced from ICIJ's official database. Pick any two (or use a curated scenario like "The Annan Network" or "Putin's Circle"), and a Jac walker agent goes to work:

1. **Pathfinding**: A bidirectional BFS discovers the shortest path between the selected figures through shell companies, intermediaries, and shared registered addresses across jurisdictions like BVI, Seychelles, Samoa, and Panama.

2. **Agent Investigation Log**: The walker narrates every decision it makes — which entity it explores, which 48 other connections it passed on, and why it chose a particular direction. This scrolls in real time so you can watch the agent think.

3. **3D Network Visualization**: The discovered path and its surrounding entities render as an interactive 3D force-directed graph. Power Players glow red, shell companies appear in gold, intermediaries in purple. You can click any node to see its full ICIJ record and trace back to the source documents.

4. **AI Structural Summary**: A `by llm()` function (Jac's inline LLM construct) generates a factual summary of each connection — strictly narrating the graph structure without editorializing. "Ayad Allawi serves as an officer of I.M.F. Holdings Inc. (Panama), which connects through Child & Child (UK intermediary) to Berkeley Square Properties Limited (BVI), where Khalifa bin Zayed bin Sultan Al Nahyan holds an officer position."

## How we built it

**Backend**: Written in the Jac programming language. The `PathFinder` walker is the core agent — it receives target Power Player IDs, uses a Python BFS module as a tool to discover paths in SQLite, materializes the results as a Jac graph (Entity nodes + LinkedTo edges), traverses the graph node by node collecting data, then calls `by llm()` to generate structural summaries. The walker carries state across traversal and reports its findings — computation happens *at* the data, not about it.

**Data Layer**: 2M+ nodes and 3.3M edges from ICIJ's public CSV exports, ingested into SQLite with FTS5 full-text search indexes. A Python module handles bidirectional BFS pathfinding, neighbor queries for the agent's decision log, and a permanent LLM response cache so we never pay for the same API call twice.

**Frontend**: Single-page vanilla JS with Three.js and 3d-force-graph for the 3D visualization. The Power Player grid uses ICIJ's official portrait images. Typewriter animation on AI summaries. The agent investigation log animates entry by entry with cascade timing.

**Deployment**: Vercel serves the static frontend with rewrite rules that proxy API calls to Railway, where the Jac server runs in Docker with the SQLite database built during the container build step.

## Challenges we ran into

**Force simulation vs. progressive reveal**: We originally wanted nodes to appear one by one as the walker discovered them. But 3d-force-graph needs all nodes present for the physics simulation to spread them apart — invisible nodes cluster at (0,0,0). We had to reveal all nodes at once and use the cascade animation only for the investigation log.

**Intermediary fan-out**: The first BFS attempts returned graphs dominated by a single law firm's 68+ other clients. We added per-node fan-out caps (max 15 neighbors per expansion) and graph expansion (adding context nodes around path nodes) to produce dense, meaningful networks instead of thin chains or noisy blobs.

**Power Player connectivity**: Only 16 of 4,371 possible PP pairs were connected within 6 hops. We increased the search depth to 8 hops (finding 51 pairs) and removed selection restrictions so users can try any combination — the tool handles "no connection found" gracefully instead of preventing exploration.

**Duplicate ICIJ records**: The same person (e.g., Petro Poroshenko) can appear as multiple node IDs in the ICIJ data from different leaked documents. Without deduplication, the graph showed the same name twice, breaking the force simulation with orphan edges pointing to nodes that were removed.

## Accomplishments that we're proud of

- The agent investigation log. Watching the walker say "Passed on 48 other connections: TROY DEVELOPMENTS LIMITED, EPSILON CONTROL SYSTEMS..." makes you realize it's navigating a massive graph, not just replaying a database query. That's the moment it feels like a real investigation tool.

- The `by llm()` summaries are genuinely useful. "Ahmad Ali al-Mirghani connects to Khalifa bin Zayed bin Sultan Al Nahyan through a chain spanning three jurisdictions" — constrained to structural facts, no editorializing, every claim traceable to an ICIJ edge.

- The demo scenarios. "The Annan Network" (UN Secretary General's son, Cambodia's justice minister, Malaysian PM's son, and a Nigerian governor all sharing offshore infrastructure) produces a 51-node graph that tells a real story.

## What we learned

- **Jac's walker paradigm** is genuinely different from function calls. The PathFinder walker is a stateful agent that moves through a graph, makes decisions at each node, and carries accumulated knowledge. It's not just syntax sugar — it changes how you think about the computation.

- **`by llm()` works best with constraints.** Giving the LLM free rein to "analyze" offshore networks produces conspiracy-board nonsense. Constraining it to strictly narrate graph structure ("Person A connects to Person B through N entities across M jurisdictions") produces something credible and useful.

- **The data pipeline is 70% of the work.** Ingesting 6 CSVs, building FTS5 indexes, matching Power Players by name, pre-computing connectivity matrices, caching LLM responses — the Jac code is elegant but the Python/SQLite plumbing is what makes it work.

## What's next for ICIJ Offshore Leaks Investigator

- **Cross-leak connections**: Currently most connections are within a single leak (Panama Papers). Bridging across leaks (finding someone who appears in both Panama and Pandora Papers) would reveal longer-term patterns.

- **Smarter walker decisions**: The agent currently follows the pre-computed BFS path. A truly autonomous walker could use heuristics or LLM reasoning to explore directions the BFS didn't consider — potentially discovering alternative paths or unexpected convergence points.

- **Timeline view**: The ICIJ data includes incorporation dates and relationship start/end dates. Laying connections out chronologically ("shell company incorporated March 2004, donation made June 2004") would surface temporal patterns the graph alone doesn't show.

- **More data sources**: FEC campaign finance, congressional stock trades, lobbying disclosures. Cross-referencing ICIJ offshore records with domestic financial data would surface connections that no single database reveals on its own.
