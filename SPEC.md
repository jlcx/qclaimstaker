# SPEC — qclaimstaker v2

## Purpose

Produce **ranked, typed, directed candidate Wikidata statements** from Wikipedia link-graph evidence, such that each candidate is:

1. not already implied by existing Wikidata (directly or via inverse / transitive chain),
2. constraint-compatible with a specific property, and
3. ranked with enough confidence to route between automated batch submission and targeted human review — rather than forcing a per-pair manual y/n on the whole candidate set.

## Non-goals

- Replacing human review for ambiguous cases; it still exists at the tail.
- Proposing non-item-valued statements (dates, strings, coords, quantities).
- Proposing qualifiers, ranks, or references beyond the one provenance field described below.
- Ontology engineering (proposing new properties or classes).

## Input

- Local ALGAE Postgres, containing at minimum:
  - `wp_links(src_qid, dst_qid, wp_count)` — directed Wikipedia link counts across language editions.
  - `wd_links(src_qid, dst_qid, pid)` — existing Wikidata item-valued statements.
  - Full Wikidata entity JSON ingested as row-per-QID with JSONB claims, labels, sitelinks.
- Dump-cadence refresh (weekly or whatever ALGAE offers). All derived tables are rebuilt per dump; proposals carry the dump version that produced them.

No live `wbgetentities` in the hot path. Live API is allowed only as a fallback inside the review UI for freshly requested QIDs, with a proper on-disk cache keyed by QID.

## Candidate query (replaces `wp_not_wd.sql`)

- Threshold: `wp_count >= 20` — low enough to catch relationships documented in only a few dozen wikipedias, high enough to cut most navigational long-tail. Tunable per type-pair later (see §Ranking).
- Pairs excluded at query time if already covered after **inverse normalization** (§1) or implied by **transitive closure** (§2). Both implemented in SQL, not Python.

Output of this stage: `candidate_pairs(src, dst, wp_count)`.

## Pipeline stages

### 1. Inverse normalization

- Maintain `inverse_properties(pid_a, pid_b)` bootstrapped from Wikidata's `P1696` plus a small hand-curated seed for known-but-undeclared pairs (`P527/P361`, `P40/P22`, `P25/P40`, …).
- When checking whether `wd_links` already covers a pair, treat `(a, P_x, b)` as also covering `(b, P_y, a)` for every `P_y` that is inverse of `P_x`. Prevents proposing the missing half of an already-stated pair.

### 2. Transitive closure pruning

- Iterative frontier BFS over a configurable set of transitive properties: `P361`, `P527`, `P131`, `P276`, `P279`, `P171`. Depth bound default **3** — the original spec said 6, but at full Wikidata scale those 6 PIDs together form a densely connected graph and the frontier roughly triples per round, so depth 6 yields billions of pairs with little additional pruning signal beyond depth 3. The `P31 ∘ P279*` composition that this section originally folded into the walk is handled inline at candidate time via `direct_types ⋈ subclass_closure`, since its materialized form is ~1B rows at scale.
- Drop any candidate pair where the relationship is already implied along such a chain.
- Materialized as a view refreshed per dump. The v1 `has_any_path` Python walker is removed; that work belongs in the database.

### 3. Type resolution

- For every entity, materialize two sets:
  - `direct_types(qid, type_qid)` — raw `P31` values.
  - `type_closure(qid, type_qid)` — conceptually `P31` followed by `P279*`, but **not materialized**. Even restricted to ancestor types that appear in property constraints or blocklists, the deduped join over 126 M `direct_types` rows spills tens of terabytes of temp and takes many hours. Every use site instead joins `direct_types ⋈ subclass_closure` inline via their PK indexes (two lookups per check). The `type_closure` table is kept empty for schema stability.
- Aggregation for histograms uses `direct_types`. Constraint matching uses `type_closure`. This fixes the v1 fracture where equivalent subtypes (e.g., "species of dinosaur" vs. "taxon") were counted separately and failed constraint matches expecting a superclass.

### 4. Noise filtering

All exclusions live in DB tables with a `reason` column, not in Python dicts.

- `meta_type_blocklist(qid, reason)`: Wikimedia category, list article, disambiguation page, set index article, Wikimedia template, scholarly article (`Q13442814`), Wikimedia internal item, project page, calendar unit, year, leap year, etc. A pair is dropped if either side's `type_closure` intersects this set.
- `source_blocklist(qid, reason)` / `destination_blocklist(qid, reason)`: the v1 `exc_src`/`exc_dst` cases ("continent" does not link to continents, etc.), populated as discovered.
- Drop pairs where `src ∈ dst.type_closure` or vice versa — one is already a declared type of the other, so the "missing" link is a type assertion either already made or more appropriately routed through P31.
- Drop pairs where one side is a Wikipedia redirect target of the other in any language (requires sitelink + redirect data from the dump).

### 5. Property candidate generation

Build `property_constraints(pid, subject_types jsonb, value_types jsonb, conflicts_with jsonb, one_of jsonb, requires jsonb, exceptions jsonb, usage_count int, inverse_pid)` by parsing each property's `P2302` claims:

- Subject-type constraint (`Q21503250`): classes in qualifier `P2308`; relation mode in `P2309`.
- Value-type constraint (`Q21510865`): classes in qualifier `P2308`; relation mode in `P2309`. (An earlier draft of this spec claimed `P2309` carried the class — empirically it carries the relation mode only, same as for subject-type.)
- Conflicts-with (`Q21502838`): qualifier `P2305`.
- Item-requires-statement (`Q21503247`), one-of (`Q21510859`), value-requires-statement (`Q21510864`).
- Exception-to-constraint (`P2303`).

For each surviving `(src, dst)`, the candidate property set consists of properties `P` such that:

- some `t_s ∈ src.type_closure` satisfies `P`'s subject-type constraint (or `P` has none), and
- some `t_v ∈ dst.type_closure` satisfies `P`'s value-type constraint (or `P` has none), and
- no `P`-conflicts-with statement is already present on `src`, and
- any `P`-item-requires-statement is satisfied on `src`, and
- if `P` has a `one_of` value list, `dst` is in it.

For each candidate, also carry its `inverse_pid` so ranking and review can present the pair in its most natural direction regardless of the Wikipedia link direction.

### 6. Ranking

Per-pair candidate score, per property `P`:

- **Type-pair prior** `P(P | src_direct_types, dst_direct_types)` — estimated from the empirical distribution of existing `(src, dst, P)` triples in `wd_links` whose endpoints have matching direct types. Additive smoothing to handle rare type pairs.
- **Constraint specificity bonus** — favor properties whose declared subject/value types sit close to `src`/`dst` in the subclass graph over properties that match only via `any` / the top of the hierarchy.
- **Global usage prior** — gentle penalty on hyper-generic properties (`P31`, `P279`, `P361`) so more specific alternatives rank above them on ties. These properties are also force-routed to review regardless of score; see §7.
- **Evidence weight** — log-scaled `wp_count`.

Output: top-k candidates per pair (default k=3) with score and margin-to-next.

### 7. Output tiers

Every candidate pair lands in exactly one of:

- `auto_queue` — top-1 score above a calibrated threshold **and** margin-over-next above a calibrated threshold **and** the property is not in the always-review set (`P31`, `P279`, `P361`, `P527`, or any property flagged sensitive). Emitted directly as QuickStatements / WikibaseIntegrator batches with the standard reference "inferred from Wikipedia link graph, dump version X".
- `review_queue` — ambiguous (low margin), mid-confidence, or always-review properties. CSV + thin web UI; reviewer sees src/dst labels, top-k ranked properties with scores, a free-text "other property" field, and a reject button with reason. Writes to `reviews`.
- `rejected_auto` — no property survived §5. Logged with reason for later analysis and for growing the blocklists.

## Data model

```
candidate_pairs(src, dst, wp_count, stage, reason)
inverse_properties(pid_a, pid_b)
meta_type_blocklist(qid, reason)
source_blocklist(qid, reason)
destination_blocklist(qid, reason)
direct_types(qid, type_qid)
type_closure(qid, type_qid)  -- materialized, refreshed per dump
property_constraints(pid, subject_types jsonb, value_types jsonb,
                     conflicts_with jsonb, one_of jsonb, requires jsonb,
                     exceptions jsonb, usage_count int, inverse_pid)
proposals(src, dst, pid, score, margin, tier, dump_version, created_at)
reviews(src, dst, pid, decision, reviewer, reviewed_at, note)
```

All derived tables are rebuilt from the dump; none of them is the authoritative record. The authoritative artifacts are (a) Wikidata itself, and (b) the `reviews` table.

## Open questions

- Target precision for `auto_queue`. 99% is a plausible bar for "don't need a human"; calibration needs a labeled dev set of 500–2,000 reviewer-adjudicated pairs stratified by type-pair and property.
- Policy for `P31`, `P279`, `P361`, `P527` proposals: forced to `review_queue` in v1. Revisit after measuring review precision on them.
- How aggressively to prune via transitive closure when the implied chain uses `any`-level constraints vs. a specific one.
- How to handle pairs whose best property is *symmetric with no declared inverse* (e.g., partner, sibling) — propose both directions, or dedupe to one?
- Re-run / invalidation: stamp proposals with `dump_version`; on next run, reconcile against Wikidata's current state and expire stale queue entries.

## Out of scope for the initial cut

- Non-item-valued properties.
- Cross-wiki sitelink repair.
- Per-language evidence weighting (e.g., "this pair is only linked in Slavic wikipedias").
- A reviewer leaderboard / multi-reviewer adjudication workflow. Single-reviewer is fine for v1.
