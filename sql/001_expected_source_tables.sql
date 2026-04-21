-- Tables expected from ALGAE ingestion. qclaimstaker reads these; it does not
-- populate them. CREATE TABLE uses IF NOT EXISTS so a dev DB can be spun up
-- empty, but no indexes are created here — building a non-concurrent index
-- on a 900M-row table would hang init-db for hours and lock writes. See
-- sql/005_recommended_source_indexes.sql for the set the pipeline benefits
-- from; run those out-of-band with CREATE INDEX CONCURRENTLY.

CREATE TABLE IF NOT EXISTS wp_links (
  src text NOT NULL,
  dst text NOT NULL,
  wp_count integer NOT NULL,
  PRIMARY KEY (src, dst)
);

-- wd_links must include P31 and P279 rows (they drive direct_types and
-- subclass_closure). Properties flagged transitive in config.py must also be
-- present if their chains should be used for §2 pruning.
CREATE TABLE IF NOT EXISTS wd_links (
  src text NOT NULL,
  dst text NOT NULL,
  prop text NOT NULL,
  PRIMARY KEY (src, dst, prop)
);

-- Property entities with full JSON. ~12k rows. Required, because:
--   * P2302 property-constraint qualifiers (P2308, P2309, P2305, P2306, P2303)
--     cannot be represented as (src, dst, pid) rows in wd_links.
--   * P1696 inverse declarations (could be sourced from wd_links too, but
--     we already need the JSON for constraints).
--   * English labels for the review UI.
CREATE TABLE IF NOT EXISTS wd_properties (
  pid text PRIMARY KEY,
  labels jsonb,
  claims jsonb,
  dump_version text NOT NULL
);
CREATE INDEX IF NOT EXISTS wd_properties_dump_idx ON wd_properties(dump_version);

-- Multi-lingual labels used only by the review UI. Optional.
-- Missing rows are fine; the UI falls back to the bare QID.
CREATE TABLE IF NOT EXISTS wd_labels (
  lang varchar NOT NULL,
  label varchar NOT NULL,
  qid varchar(11) NOT NULL,
  PRIMARY KEY (qid, lang)
);
-- No lang index created here: the review UI filters by (qid, lang) and ALGAE's
-- existing idx_wd_labels_qid handles that fine. A solo lang index on a large
-- labels table would be slow to build and not useful.
