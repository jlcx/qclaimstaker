# qclaimstaker

Ranked, typed, directed candidate Wikidata statements, mined from the Wikipedia
link graph against existing Wikidata.

For each `(src_qid, dst_qid)` pair that is linked between Wikipedia articles
but not yet connected in Wikidata (even transitively, even via inverse
properties), qclaimstaker proposes candidate properties `P` consistent with
Wikidata's own constraint system, scores them, and routes each pair to one of:

- `auto_queue` — high-confidence, non-sensitive; emitted as a QuickStatements
  v2 CSV batch.
- `review_queue` — ambiguous, mid-confidence, or structurally sensitive
  (`P31`, `P279`, `P361`, `P527`); served via a small FastAPI review UI.
- `rejected_auto` — no property survived constraint filtering; logged for
  blocklist growth.

See `SPEC.md` for the design.

## Requirements

- Python ≥ 3.11
- PostgreSQL with an existing **ALGAE** database containing:
  - `wp_links(src, dst, wp_count)` — directed Wikipedia link counts.
  - `wd_links(src, dst, prop)` — existing Wikidata item-valued statements.
  - `wd_labels(qid, lang, label)` — labels (optional, used by the review UI).
- A JSON dump of Wikidata **properties** (not full entities) — e.g.
  `wikidata_properties_full.json` in the project root. Full item JSON is *not*
  required; `P31` is read out of `wd_links` directly.

## Install

```bash
pip install -e .
```

Configure the DB connection via env (prefix `QCS_`) or `.env`:

```
QCS_DSN=postgresql:///algae
QCS_DUMP_VERSION=2026-04-15
```

All knobs are in `qclaimstaker/config.py` (thresholds, transitive PIDs, weights,
review bind, etc.).

## One-time setup

The first two steps build schema and indexes. The rest of the pipeline is
re-runnable per dump.

### 1. Apply schema

```bash
qclaimstaker init-db
```

Applies `sql/001_*` through `sql/0NN_*` in order. Idempotent. This creates only
*derived* tables and PL/pgSQL functions — it does **not** create indexes on
`wp_links` / `wd_links` (those are ALGAE-owned and can take hours on full-size
tables).

### 2. Build the two required indexes on `wd_links`

These are essential for the pipeline. See
`sql/005_recommended_source_indexes.sql` for the DDL. Run each from `psql`
**outside any transaction** with `CONCURRENTLY` so writers aren't blocked:

```bash
psql -d algae -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wd_links_prop     ON wd_links (prop);"
psql -d algae -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wd_links_src_prop ON wd_links (src, prop);"
```

Each can take hours on a 900M-row table. Monitor with:

```sql
SELECT now() - query_start AS elapsed, state, wait_event, query
FROM pg_stat_activity
WHERE query LIKE 'CREATE INDEX%';
```

### 3. Load property JSON

```bash
qclaimstaker load-properties --path wikidata_properties_full.json --dump-version 2026-04-15
```

This populates `wd_properties(pid, labels, claims, dump_version)` from a
`wbgetentities`-style JSON dump. Expect ~12k rows.

### 4. Parse P2302 constraints

```bash
qclaimstaker load-constraints
```

Parses each property's `P2302` statements into `property_constraints`
(subject types, value types, conflicts-with, one-of, requires,
value-requires, exceptions, inverse PID). Also fills `usage_count` from
`wd_links`. Takes a few minutes.

### 5. Seed + harvest inverse properties

```bash
qclaimstaker load-inverses
```

Combines a hand-curated seed (`sql/003_seed_inverses.sql`) with `P1696`
declarations harvested from `wd_properties.claims`. Idempotent —
`ON CONFLICT DO NOTHING`, so re-runs report `0 added`.

Sanity check:

```sql
SELECT source, COUNT(*) FROM inverse_properties GROUP BY source;
-- expect ~220 P1696 + ~16 seed
```

## Per-dump pipeline

Each of the steps below can be re-run when a new dump lands.

### 6. Type resolution

```bash
qclaimstaker rebuild-types
```

Three phases:
- `direct_types` — `DISTINCT src, dst FROM wd_links WHERE prop='P31'`
  (fast with `idx_wd_links_prop`).
- `p279_edges` — small local cache of P279 edges from `wd_links`; the walk
  joins against this instead of the 900 M-row parent.
- `subclass_closure` — iterative BFS over `p279_edges`, depth capped by
  `QCS_SUBCLASS_MAX_DEPTH` (default 10). Emits `NOTICE` per depth.

`type_closure` is **not** materialized; every use joins
`direct_types ⋈ subclass_closure` inline. See the header of
`sql/010_subclass_closure.sql` for the reasoning.

Sanity check:

```sql
SELECT COUNT(*) FROM direct_types;
SELECT COUNT(*) FROM p279_edges;
SELECT COUNT(*) FROM subclass_closure;
SELECT COUNT(*) FROM subclass_closure WHERE sub_qid = super_qid;
SELECT COUNT(*) FROM subclass_closure WHERE sub_qid = 'Q5';
```

Expected shapes on full Wikidata: direct_types ~100–130 M, p279_edges
~3–6 M, subclass_closure a few hundred M (half direct_types self-pairs,
half real closure). The self-pair count should be in the millions and
`Q5` should report ~25–30 ancestor rows. A `COUNT(*)` that looks
plausible while self-pairs are 0 silently breaks every downstream
stage — see "Known gotchas".

### 7. Transitive closure

```bash
qclaimstaker refresh-transitive
```

Two phases:

- `transitive_edges` — a deduped local cache of `wd_links` rows whose
  `prop` is in `QCS_TRANSITIVE_PIDS` (default
  `P361 P527 P131 P276 P279 P171`). One seq scan of `wd_links`.
- `transitive_paths` — iterative frontier BFS over `transitive_edges` up to
  `QCS_TRANSITIVE_MAX_DEPTH` (default 3 — the 6 transitive PIDs together
  form a densely connected graph; the frontier roughly triples per round,
  so depth 6 yields billions of pairs with little additional pruning
  signal beyond depth 3). Emits `NOTICE` per depth. Feeds
  the pruning step in `refresh-candidates`.

The P31∘P279* composition that the original spec folded into
`transitive_paths` is handled inline in `refresh-candidates` instead —
the materialized join is prohibitively large at Wikidata scale.

### 8. Candidates + priors

```bash
qclaimstaker refresh-candidates --dump-version 2026-04-15
```

`refresh-candidates` runs three plpgsql functions in order:

1. `refresh_candidate_pairs` — pairs above `QCS_MIN_WP_COUNT` that are
   not covered directly, via inverses, or transitively, and not
   type-assertion near-duplicates. Stages A–H, one `NOTICE` per stage.
2. `refresh_type_pair_prior` — populates `type_pair_prior(src_type,
   dst_type, pid, n_obs)` from `wd_links`. Independent of
   `candidate_pairs`; depends only on `wd_links`. Pass `--skip-prior` if
   you've already built it for the current `wd_links` state and just
   want to re-run properties.
3. `refresh_candidate_properties` — per pair × `P` candidate set,
   **gated by `type_pair_prior`**: a `(src, dst, pid)` is only seeded
   if some direct-type combination of the pair has at least one
   observation of `pid` in `wd_links`. Constraint-type compatibility
   (subject_types / value_types / one_of / exceptions / conflicts_with /
   requires) is then applied as a refinement. Stages 1–8, one `NOTICE`
   per stage. The earlier constraint-only seed exploded combinatorially
   at full Wikidata scale (common types like `Q5` are subject_types of
   hundreds of properties); the prior-gated seed bounds the per-pair
   candidate set to pids with empirical support — typically tens, not
   thousands.

Standalone refresh of just the prior:

```bash
qclaimstaker refresh-prior
```

### 9. Rank + tier

```bash
qclaimstaker rank --dump-version 2026-04-15
```

Scores each candidate (type-pair prior + constraint specificity + log-wp
evidence − log-usage penalty on hyper-generic properties), keeps top-k per
pair, and assigns each pair to exactly one of `auto_queue` /
`review_queue` / `rejected_auto`. Properties in `QCS_ALWAYS_REVIEW`
(default `P31 P279 P361 P527`) are force-routed to review.

### 10. Emit QuickStatements CSV

```bash
qclaimstaker emit-qs --dump-version 2026-04-15 --out-dir out
```

Writes a QuickStatements v2 CSV of the `auto_queue` rows with the standard
reference `QCS_QS_REFERENCE_TEMPLATE` (default: *"inferred from Wikipedia
link graph, dump {dump_version}"*).

### 11. Review UI

```bash
qclaimstaker serve
```

FastAPI on `QCS_REVIEW_BIND:QCS_REVIEW_PORT` (default 127.0.0.1:8765).
Shows src/dst labels, top-k ranked properties with scores, a free-text
"other property" field, and a reject-with-reason button. Decisions land in
`reviews`.

## End-to-end convenience

```bash
qclaimstaker run-all 2026-04-15
```

Runs steps 1 + 6–10 in sequence. Assumes `wp_links`, `wd_links`,
`wd_properties`, and (optionally) `wd_labels` are already populated, and
that both `idx_wd_links_*` exist.

## Layout

```
sql/
  001_expected_source_tables.sql   ALGAE contract (no index DDL)
  002_derived_tables.sql           all qclaimstaker-owned tables
  003_seed_inverses.sql            hand-curated inverse pairs
  004_seed_blocklists.sql          meta / source / destination blocklists
  005_recommended_source_indexes.sql  CONCURRENTLY examples (manual)
  010_subclass_closure.sql         refresh_subclass_closure / refresh_type_closure
  011_transitive_paths.sql         refresh_transitive_paths
  012_candidate_refresh.sql        refresh_candidate_pairs / _properties / _prior
qclaimstaker/
  cli.py                           Typer entry point
  config.py                        pydantic-settings
  db.py                            psycopg connection helper
  pipeline/
    schema.py                      applies sql/*.sql in order
    load_properties.py             ingests wikidata_properties_full.json
    constraints.py                 P2302 → property_constraints
    inverse.py                     P1696 + seed → inverse_properties
    type_resolution.py             direct_types + closures
    candidates.py                  wraps the candidate SQL functions
    ranking.py                     scoring formula
    tiering.py                     auto / review / rejected assignment
    emitter.py                     QuickStatements v2 CSV
  review/
    app.py                         FastAPI review UI
    templates/                     Jinja2
```

## Notes on scale

- `wd_links` is ~900M rows. Anything that scans it without an index will
  hurt. The two `idx_wd_links_*` indexes above are load-bearing.
- `init-db` itself is fast (seconds) — it only touches small derived
  tables. If it seems to hang, something in `sql/` is accidentally
  touching a source table.
- Property JSON (`wikidata_properties_full.json`, ~465 MB) is loaded in
  one pass with `json.load`. Full *item* JSON is intentionally not
  required or stored.

## Known gotchas

- **P2309 is not classes.** The earlier spec claimed value-type
  constraints stored classes in `P2309`; empirically `P2309` is always the
  relation mode, and `P2308` carries the classes for both subject- and
  value-type constraints. `constraints.py` and `SPEC.md` are corrected.
- **Column names.** `wd_links` columns are `src, dst, prop` (not `pid` or
  `_qid` variants). Same for `wp_links`.
- **Re-running `load-inverses` reports 0 added** — that's correct; every
  row hits `ON CONFLICT DO NOTHING` the second time.
- **`refresh-candidates` reports `src_match: 0` / `dst_match: 0`.**
  `subclass_closure` is empty or missing self-pairs (a previously
  cancelled rebuild can leave it that way). Rebuild directly with
  `psql -d algae -c 'SELECT refresh_subclass_closure(10);'` — it
  TRUNCATEs and walks p279_edges in one shot. Watch the per-depth
  NOTICEs; closure should converge by depth ~7 and finish at ~250 M
  rows. Then re-run `refresh-candidates`.
