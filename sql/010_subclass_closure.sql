-- Build subclass_closure via an iterative frontier BFS over p279_edges (a
-- small local cache of wd_links WHERE prop='P279'; see 009_edge_cache.sql).
-- Each round inserts only newly-reached (sub, super) pairs; the frontier
-- shrinks to empty and we exit early.
--
-- Notes on performance traps avoided:
--   (1) WITH RECURSIVE + GROUP BY over P279 at depth 10 materializes a
--       multi-hundred-million-row intermediate set and spends hours in
--       WALWrite upserting it. The iterative form below bounds each round.
--   (2) Probing a 900M-row wd_links(src,prop) index millions of times per
--       round is I/O-bound. p279_edges fits in shared_buffers.
--   (3) The depth-0 self-pairs must NOT do a 126M-row DISTINCT scan of
--       direct_types inside a UNION — that alone will run for hours.
--       BFS-needed self-pairs only come from the P279 graph nodes (a few
--       million). direct_types self-pairs are added once at the very end,
--       after the walk, and are only needed for classes that have no P279
--       edges at all (rare edge case).
--   (4) NOT EXISTS against a growing subclass_closure becomes a nested loop
--       with PK lookup; under default work_mem Postgres won't choose a
--       hash anti-join. We raise work_mem and phrase the anti-join as
--       LEFT JOIN ... IS NULL to nudge it in the right direction.

CREATE OR REPLACE FUNCTION refresh_subclass_closure(p_max_depth int DEFAULT 10)
RETURNS integer AS $$
DECLARE
  d int;
  added int;
  total int;
BEGIN
  SET LOCAL work_mem = '2GB';

  TRUNCATE subclass_closure;

  CREATE TEMP TABLE _sc_cur (
    sub_qid text NOT NULL,
    super_qid text NOT NULL,
    PRIMARY KEY (sub_qid, super_qid)
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _sc_nxt (
    sub_qid text NOT NULL,
    super_qid text NOT NULL,
    PRIMARY KEY (sub_qid, super_qid)
  ) ON COMMIT DROP;

  RAISE NOTICE 'depth 0: self-pairs over p279 graph nodes';

  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT DISTINCT src, src, 0 FROM p279_edges
  ON CONFLICT DO NOTHING;

  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT DISTINCT dst, dst, 0 FROM p279_edges
  ON CONFLICT DO NOTHING;

  SELECT COUNT(*) INTO total FROM subclass_closure;
  RAISE NOTICE 'depth 0 done; closure=%', total;

  -- Depth 1: direct P279 edges are the first frontier.
  INSERT INTO _sc_cur (sub_qid, super_qid)
  SELECT DISTINCT src, dst FROM p279_edges
  ON CONFLICT DO NOTHING;
  ANALYZE _sc_cur;

  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT sub_qid, super_qid, 1 FROM _sc_cur
  ON CONFLICT (sub_qid, super_qid) DO UPDATE
    SET depth = LEAST(subclass_closure.depth, 1);

  GET DIAGNOSTICS added = ROW_COUNT;
  SELECT COUNT(*) INTO total FROM subclass_closure;
  RAISE NOTICE 'depth 1 done; frontier=%, closure=%', added, total;

  -- Depths 2..max: extend one hop at a time, keep only newly-reached pairs.
  FOR d IN 2..p_max_depth LOOP
    TRUNCATE _sc_nxt;

    INSERT INTO _sc_nxt (sub_qid, super_qid)
    SELECT DISTINCT c.sub_qid, e.dst
    FROM _sc_cur c
    JOIN p279_edges e ON e.src = c.super_qid
    LEFT JOIN subclass_closure sc
      ON sc.sub_qid = c.sub_qid AND sc.super_qid = e.dst
    WHERE sc.sub_qid IS NULL
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS added = ROW_COUNT;
    EXIT WHEN added = 0;

    INSERT INTO subclass_closure (sub_qid, super_qid, depth)
    SELECT sub_qid, super_qid, d FROM _sc_nxt;

    TRUNCATE _sc_cur;
    INSERT INTO _sc_cur (sub_qid, super_qid)
    SELECT sub_qid, super_qid FROM _sc_nxt;
    ANALYZE _sc_cur;

    SELECT COUNT(*) INTO total FROM subclass_closure;
    RAISE NOTICE 'depth % done; frontier=%, closure=%', d, added, total;
  END LOOP;

  -- Finally, self-pairs for any direct_types whose type is not in the P279
  -- graph (isolated classes). Runs once, post-BFS, against direct_types_type_idx.
  RAISE NOTICE 'adding direct_types self-pairs (may take a few min on full Wikidata)';
  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT DISTINCT type_qid, type_qid, 0 FROM direct_types
  ON CONFLICT DO NOTHING;

  SELECT COUNT(*) INTO total FROM subclass_closure;
  RAISE NOTICE 'closure complete: %', total;

  RETURN total;
END;
$$ LANGUAGE plpgsql;


-- type_closure = direct_types ⋈ subclass_closure.
CREATE OR REPLACE FUNCTION refresh_type_closure()
RETURNS integer AS $$
DECLARE
  n integer;
BEGIN
  SET LOCAL work_mem = '2GB';
  TRUNCATE type_closure;
  INSERT INTO type_closure (qid, type_qid)
  SELECT DISTINCT dt.qid, sc.super_qid
  FROM direct_types dt
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$$ LANGUAGE plpgsql;
