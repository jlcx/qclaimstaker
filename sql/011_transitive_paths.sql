-- Pairs (src, dst) already implied by a chain over the configured transitive
-- properties, or by the P31 ∘ P279* composition. Rebuilt per dump.
--
-- NOTE: for a full Wikidata dump this MV can be very large. If it grows
-- unmanageable, partition by the first transitive step or shard by PID and
-- union the parts. For v1 we keep it simple.

CREATE OR REPLACE FUNCTION refresh_transitive_paths(
  p_transitive_pids text[] DEFAULT ARRAY['P361','P527','P131','P276','P279','P171'],
  p_max_depth int DEFAULT 6
)
RETURNS integer AS $$
DECLARE
  n integer;
BEGIN
  DROP TABLE IF EXISTS transitive_paths;
  CREATE TABLE transitive_paths (
    src text NOT NULL,
    dst text NOT NULL,
    PRIMARY KEY (src, dst)
  );

  WITH RECURSIVE walk(src, dst, depth) AS (
    SELECT src, dst, 1
    FROM wd_links
    WHERE prop = ANY(p_transitive_pids)
    UNION
    SELECT w.src, wl.dst, w.depth + 1
    FROM walk w
    JOIN wd_links wl ON wl.src = w.dst
    WHERE wl.prop = ANY(p_transitive_pids)
      AND w.depth < p_max_depth
  )
  INSERT INTO transitive_paths (src, dst)
  SELECT DISTINCT src, dst FROM walk
  ON CONFLICT DO NOTHING;

  -- P31 ∘ P279* composition: an entity is "transitively linked" to every
  -- ancestor of each of its direct types.
  INSERT INTO transitive_paths (src, dst)
  SELECT DISTINCT dt.qid, sc.super_qid
  FROM direct_types dt
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
  WHERE sc.depth > 0
  ON CONFLICT DO NOTHING;

  CREATE INDEX transitive_paths_dst_idx ON transitive_paths(dst);

  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN (SELECT COUNT(*) FROM transitive_paths);
END;
$$ LANGUAGE plpgsql;
