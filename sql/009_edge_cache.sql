-- Small, hot-path edge caches materialized from wd_links so the closure /
-- transitive walks don't hammer 900M-row wd_links indexes with millions of
-- random lookups. Each cache is bounded by the number of edges of that
-- property (P279: a few million; tiny by comparison to wd_links) and fits
-- comfortably in shared_buffers.

CREATE TABLE IF NOT EXISTS p279_edges (
  src text NOT NULL,
  dst text NOT NULL,
  PRIMARY KEY (src, dst)
);
CREATE INDEX IF NOT EXISTS p279_edges_src_idx ON p279_edges (src);


CREATE OR REPLACE FUNCTION refresh_p279_edges()
RETURNS integer AS $$
DECLARE
  n integer;
BEGIN
  TRUNCATE p279_edges;
  INSERT INTO p279_edges (src, dst)
  SELECT DISTINCT src, dst FROM wd_links WHERE prop = 'P279';
  GET DIAGNOSTICS n = ROW_COUNT;
  ANALYZE p279_edges;
  RETURN n;
END;
$$ LANGUAGE plpgsql;
