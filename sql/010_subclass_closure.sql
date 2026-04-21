-- Build subclass_closure from P279 edges in wd_links.
-- Bounded by :max_depth (default 10 in code). Runs fast because the type graph
-- (entities reachable via P279*) is small relative to the full entity graph.

CREATE OR REPLACE FUNCTION refresh_subclass_closure(p_max_depth int DEFAULT 10)
RETURNS integer AS $$
DECLARE
  n integer;
BEGIN
  TRUNCATE subclass_closure;

  -- Depth-0: every type is its own ancestor, so constraint matching treats
  -- the direct type as satisfying itself.
  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT DISTINCT type_qid, type_qid, 0 FROM direct_types;

  -- Also include any class that participates in P279 edges even if it never
  -- appears in direct_types — needed for matching constraints expressed
  -- against abstract superclasses.
  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT DISTINCT q, q, 0 FROM (
    SELECT src AS q FROM wd_links WHERE prop = 'P279'
    UNION
    SELECT dst AS q FROM wd_links WHERE prop = 'P279'
  ) s
  ON CONFLICT DO NOTHING;

  WITH RECURSIVE walk(sub_qid, super_qid, depth) AS (
    SELECT src, dst, 1
    FROM wd_links
    WHERE prop = 'P279'
    UNION
    SELECT w.sub_qid, wl.dst, w.depth + 1
    FROM walk w
    JOIN wd_links wl ON wl.src = w.super_qid AND wl.prop = 'P279'
    WHERE w.depth < p_max_depth
  )
  INSERT INTO subclass_closure (sub_qid, super_qid, depth)
  SELECT sub_qid, super_qid, MIN(depth)::smallint
  FROM walk
  GROUP BY sub_qid, super_qid
  ON CONFLICT (sub_qid, super_qid)
  DO UPDATE SET depth = LEAST(subclass_closure.depth, EXCLUDED.depth);

  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN (SELECT COUNT(*) FROM subclass_closure);
END;
$$ LANGUAGE plpgsql;


-- type_closure = direct_types ⋈ subclass_closure.
CREATE OR REPLACE FUNCTION refresh_type_closure()
RETURNS integer AS $$
DECLARE
  n integer;
BEGIN
  TRUNCATE type_closure;
  INSERT INTO type_closure (qid, type_qid)
  SELECT DISTINCT dt.qid, sc.super_qid
  FROM direct_types dt
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$$ LANGUAGE plpgsql;
