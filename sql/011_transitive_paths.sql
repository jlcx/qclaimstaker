-- Pairs (src, dst) already implied by a chain over the configured transitive
-- properties. Built via iterative frontier BFS over transitive_edges (a
-- deduped local cache of wd_links filtered to the transitive PIDs; see
-- 009_edge_cache.sql). Each round inserts only newly-reached pairs; the
-- frontier shrinks to empty and we exit early.
--
-- Notes on what this used to do and no longer does:
--   * The earlier version ran WITH RECURSIVE over wd_links WHERE prop =
--     ANY(...) at depth 6, with UNION dedup. At Wikidata scale that spills
--     tens of GB of temp per hour and eventually times out. Fixed here by
--     caching edges and walking iteratively with PK-based dedup.
--   * The earlier version also folded in the P31 ∘ P279* composition into
--     transitive_paths. That join (direct_types ⋈ subclass_closure) at full
--     scale is a 600M+ row beast whose DISTINCT spills over a terabyte. It
--     is now handled inline in refresh_candidate_pairs via direct_types +
--     subclass_closure with PK index lookups.

CREATE OR REPLACE FUNCTION refresh_transitive_paths(
  p_max_depth int DEFAULT 6
)
RETURNS integer AS $$
DECLARE
  d int;
  added int;
  total int;
BEGIN
  SET LOCAL work_mem = '2GB';

  DROP TABLE IF EXISTS transitive_paths;
  CREATE TABLE transitive_paths (
    src text NOT NULL,
    dst text NOT NULL,
    PRIMARY KEY (src, dst)
  );

  CREATE TEMP TABLE _tp_cur (
    src text NOT NULL,
    dst text NOT NULL,
    PRIMARY KEY (src, dst)
  ) ON COMMIT DROP;

  CREATE TEMP TABLE _tp_nxt (
    src text NOT NULL,
    dst text NOT NULL,
    PRIMARY KEY (src, dst)
  ) ON COMMIT DROP;

  RAISE NOTICE 'depth 1: direct transitive edges';

  INSERT INTO _tp_cur (src, dst)
  SELECT src, dst FROM transitive_edges
  ON CONFLICT DO NOTHING;
  ANALYZE _tp_cur;

  INSERT INTO transitive_paths (src, dst)
  SELECT src, dst FROM _tp_cur
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS added = ROW_COUNT;
  SELECT COUNT(*) INTO total FROM transitive_paths;
  RAISE NOTICE 'depth 1 done; frontier=%, paths=%', added, total;

  FOR d IN 2..p_max_depth LOOP
    TRUNCATE _tp_nxt;

    INSERT INTO _tp_nxt (src, dst)
    SELECT DISTINCT c.src, e.dst
    FROM _tp_cur c
    JOIN transitive_edges e ON e.src = c.dst
    LEFT JOIN transitive_paths tp
      ON tp.src = c.src AND tp.dst = e.dst
    WHERE tp.src IS NULL
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS added = ROW_COUNT;
    EXIT WHEN added = 0;

    INSERT INTO transitive_paths (src, dst)
    SELECT src, dst FROM _tp_nxt
    ON CONFLICT DO NOTHING;

    TRUNCATE _tp_cur;
    INSERT INTO _tp_cur (src, dst) SELECT src, dst FROM _tp_nxt;
    ANALYZE _tp_cur;

    SELECT COUNT(*) INTO total FROM transitive_paths;
    RAISE NOTICE 'depth % done; frontier=%, paths=%', d, added, total;
  END LOOP;

  CREATE INDEX transitive_paths_dst_idx ON transitive_paths(dst);

  SELECT COUNT(*) INTO total FROM transitive_paths;
  RAISE NOTICE 'transitive_paths complete: % rows', total;
  RETURN total;
END;
$$ LANGUAGE plpgsql;
