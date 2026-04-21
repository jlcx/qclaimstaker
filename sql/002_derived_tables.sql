-- Derived tables owned by qclaimstaker. All are rebuilt per dump; none is the
-- authoritative record. The authoritative artifacts are (a) Wikidata itself
-- and (b) the `reviews` table.

CREATE TABLE IF NOT EXISTS dump_versions (
  dump_version text PRIMARY KEY,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  notes text
);

CREATE TABLE IF NOT EXISTS inverse_properties (
  pid_a text NOT NULL,
  pid_b text NOT NULL,
  source text NOT NULL,  -- 'P1696' or 'seed'
  PRIMARY KEY (pid_a, pid_b)
);
CREATE INDEX IF NOT EXISTS inverse_properties_b_idx ON inverse_properties(pid_b);

CREATE TABLE IF NOT EXISTS meta_type_blocklist (
  qid text PRIMARY KEY,
  reason text NOT NULL
);

CREATE TABLE IF NOT EXISTS source_blocklist (
  qid text PRIMARY KEY,
  reason text NOT NULL
);

CREATE TABLE IF NOT EXISTS destination_blocklist (
  qid text PRIMARY KEY,
  reason text NOT NULL
);

-- Raw P31 values per entity.
CREATE TABLE IF NOT EXISTS direct_types (
  qid text NOT NULL,
  type_qid text NOT NULL,
  PRIMARY KEY (qid, type_qid)
);
CREATE INDEX IF NOT EXISTS direct_types_type_idx ON direct_types(type_qid);

-- Transitive closure of P279 on the type graph. Smaller than the entity
-- closure by orders of magnitude; join with direct_types to reach entities.
CREATE TABLE IF NOT EXISTS subclass_closure (
  sub_qid text NOT NULL,
  super_qid text NOT NULL,
  depth smallint NOT NULL,
  PRIMARY KEY (sub_qid, super_qid)
);
CREATE INDEX IF NOT EXISTS subclass_closure_super_idx ON subclass_closure(super_qid);

-- direct_types ⋈ subclass_closure: one row per (entity, any ancestor type).
CREATE TABLE IF NOT EXISTS type_closure (
  qid text NOT NULL,
  type_qid text NOT NULL,
  PRIMARY KEY (qid, type_qid)
);
CREATE INDEX IF NOT EXISTS type_closure_type_idx ON type_closure(type_qid);

-- Parsed P2302 constraints per property, plus usage count and inverse pid.
-- jsonb columns default to null when the property lacks that constraint.
CREATE TABLE IF NOT EXISTS property_constraints (
  pid text PRIMARY KEY,
  label text,
  subject_types jsonb,    -- [qid, ...]
  value_types jsonb,      -- [qid, ...]
  conflicts_with jsonb,   -- [{pid, values?: [qid, ...]}, ...]
  one_of jsonb,           -- [qid, ...]
  requires jsonb,         -- [{pid, values?: [qid, ...]}, ...]
  value_requires jsonb,   -- [{pid, values?: [qid, ...]}, ...]
  exceptions jsonb,       -- [qid, ...] (P2303)
  usage_count integer NOT NULL DEFAULT 0,
  inverse_pid text
);

CREATE TABLE IF NOT EXISTS candidate_pairs (
  src text NOT NULL,
  dst text NOT NULL,
  wp_count integer NOT NULL,
  dump_version text NOT NULL,
  PRIMARY KEY (src, dst, dump_version)
);
CREATE INDEX IF NOT EXISTS candidate_pairs_dump_idx ON candidate_pairs(dump_version);

CREATE TABLE IF NOT EXISTS candidate_properties (
  src text NOT NULL,
  dst text NOT NULL,
  pid text NOT NULL,
  direction text NOT NULL CHECK (direction IN ('forward', 'inverse')),
  dump_version text NOT NULL,
  PRIMARY KEY (src, dst, pid, dump_version)
);
CREATE INDEX IF NOT EXISTS candidate_properties_dump_idx ON candidate_properties(dump_version);

CREATE TABLE IF NOT EXISTS proposals (
  src text NOT NULL,
  dst text NOT NULL,
  pid text NOT NULL,
  score double precision NOT NULL,
  margin double precision NOT NULL,
  tier text NOT NULL CHECK (tier IN ('auto_queue', 'review_queue', 'rejected_auto')),
  direction text NOT NULL CHECK (direction IN ('forward', 'inverse')),
  rank smallint NOT NULL,
  wp_count integer NOT NULL,
  dump_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (src, dst, pid, dump_version)
);
CREATE INDEX IF NOT EXISTS proposals_tier_idx ON proposals(tier, dump_version);
CREATE INDEX IF NOT EXISTS proposals_pair_idx ON proposals(src, dst, dump_version, rank);

CREATE TABLE IF NOT EXISTS reviews (
  src text NOT NULL,
  dst text NOT NULL,
  pid text NOT NULL,
  decision text NOT NULL CHECK (decision IN ('accept', 'reject', 'other')),
  other_pid text,
  reject_reason text,
  reviewer text NOT NULL,
  reviewed_at timestamptz NOT NULL DEFAULT now(),
  note text,
  PRIMARY KEY (src, dst, pid, reviewed_at)
);
CREATE INDEX IF NOT EXISTS reviews_pair_idx ON reviews(src, dst);

CREATE TABLE IF NOT EXISTS rejected_pairs (
  src text NOT NULL,
  dst text NOT NULL,
  reason text NOT NULL,
  dump_version text NOT NULL,
  PRIMARY KEY (src, dst, dump_version)
);
