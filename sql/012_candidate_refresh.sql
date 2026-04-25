-- §Candidate query + §5 Property candidate generation, both in SQL.

CREATE OR REPLACE FUNCTION refresh_candidate_pairs(
  p_dump text,
  p_min_wp_count int DEFAULT 20
)
RETURNS integer AS $$
DECLARE n integer;
BEGIN
  DELETE FROM candidate_pairs WHERE dump_version = p_dump;
  DELETE FROM rejected_pairs  WHERE dump_version = p_dump;

  WITH
  -- §1: inverse-normalized coverage. (a, P, b) covers both (a, b) and
  -- (b, a) for every P_y declared inverse of P.
  wd_covered AS (
    SELECT src, dst FROM wd_links
    UNION
    SELECT wl.dst AS src, wl.src AS dst
    FROM wd_links wl
    WHERE EXISTS (SELECT 1 FROM inverse_properties ip WHERE ip.pid_a = wl.prop)
  ),
  raw AS (
    SELECT src, dst, wp_count
    FROM wp_links
    WHERE wp_count >= p_min_wp_count
  ),
  filtered AS (
    SELECT r.src, r.dst, r.wp_count
    FROM raw r
    WHERE NOT EXISTS (
            SELECT 1 FROM wd_covered c
            WHERE c.src = r.src AND c.dst = r.dst)
      AND NOT EXISTS (
            SELECT 1 FROM transitive_paths t
            WHERE t.src = r.src AND t.dst = r.dst)
      -- P31 ∘ P279* composition: dst is already an ancestor-type of src's
      -- direct type(s). Inlined rather than folded into transitive_paths
      -- because materializing that join at Wikidata scale spills terabytes.
      AND NOT EXISTS (
            SELECT 1
            FROM direct_types dt
            JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
            WHERE dt.qid = r.src AND sc.super_qid = r.dst AND sc.depth > 0)
      AND NOT EXISTS (
            SELECT 1 FROM meta_type_blocklist m
            JOIN direct_types dt ON dt.qid = r.src OR dt.qid = r.dst
            JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
                                    AND sc.super_qid = m.qid)
      AND NOT EXISTS (SELECT 1 FROM source_blocklist sb      WHERE sb.qid = r.src)
      AND NOT EXISTS (SELECT 1 FROM destination_blocklist db WHERE db.qid = r.dst)
      -- type-assertion dedupe: one side is already a declared type of the
      -- other. Goes through direct_types+subclass_closure rather than
      -- type_closure because type_closure is (intentionally) restricted to
      -- types that property constraints / blocklists reference, whereas
      -- this check needs every possible QID-as-type.
      AND NOT EXISTS (
            SELECT 1
            FROM direct_types dt
            JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
            WHERE dt.qid = r.src AND sc.super_qid = r.dst)
      AND NOT EXISTS (
            SELECT 1
            FROM direct_types dt
            JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
            WHERE dt.qid = r.dst AND sc.super_qid = r.src)
  )
  INSERT INTO candidate_pairs (src, dst, wp_count, dump_version)
  SELECT src, dst, wp_count, p_dump FROM filtered;

  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$$ LANGUAGE plpgsql;


-- §5: for each surviving pair, enumerate properties whose constraints match.
-- Matching is done against type_closure; exclusion against wd_links (forward
-- statement already present) and inverse_properties (statement already present
-- in reverse direction on an inverse pid).
CREATE OR REPLACE FUNCTION refresh_candidate_properties(p_dump text)
RETURNS integer AS $$
DECLARE n integer;
BEGIN
  DELETE FROM candidate_properties WHERE dump_version = p_dump;

  INSERT INTO candidate_properties (src, dst, pid, direction, dump_version)
  SELECT cp.src, cp.dst, pc.pid, 'forward', p_dump
  FROM candidate_pairs cp
  JOIN property_constraints pc ON TRUE
  WHERE cp.dump_version = p_dump
    AND (pc.subject_types IS NULL OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(pc.subject_types) st(type_qid)
          JOIN direct_types dt ON dt.qid = cp.src
          JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
                                  AND sc.super_qid = st.type_qid))
    AND (pc.value_types IS NULL OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(pc.value_types) vt(type_qid)
          JOIN direct_types dt ON dt.qid = cp.dst
          JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
                                  AND sc.super_qid = vt.type_qid))
    AND (pc.exceptions IS NULL OR NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(pc.exceptions) e(qid)
          WHERE e.qid = cp.src OR e.qid = cp.dst))
    AND (pc.one_of IS NULL OR EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(pc.one_of) o(qid)
          WHERE o.qid = cp.dst))
    AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(COALESCE(pc.conflicts_with,'[]'::jsonb)) c(c)
          JOIN wd_links wl ON wl.src = cp.src AND wl.prop = (c.c->>'pid'))
    AND (pc.requires IS NULL OR NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(pc.requires) r(r)
          WHERE NOT EXISTS (
            SELECT 1 FROM wd_links wl
            WHERE wl.src = cp.src AND wl.prop = (r.r->>'pid'))))
    AND NOT EXISTS (
          SELECT 1 FROM wd_links wl
          WHERE wl.src = cp.src AND wl.dst = cp.dst AND wl.prop = pc.pid)
    -- also skip if an inverse of pc.pid already covers the reverse direction
    AND NOT EXISTS (
          SELECT 1
          FROM inverse_properties ip
          JOIN wd_links wl ON wl.prop = ip.pid_b
          WHERE ip.pid_a = pc.pid
            AND wl.src = cp.dst AND wl.dst = cp.src);

  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$$ LANGUAGE plpgsql;


-- Per-dump: count (src_type, dst_type, pid) triples in existing wd_links.
-- Used by Python ranking with additive smoothing.
CREATE OR REPLACE FUNCTION refresh_type_pair_prior()
RETURNS integer AS $$
BEGIN
  DROP TABLE IF EXISTS type_pair_prior;
  CREATE TABLE type_pair_prior (
    src_type text NOT NULL,
    dst_type text NOT NULL,
    pid text NOT NULL,
    n_obs integer NOT NULL,
    PRIMARY KEY (src_type, dst_type, pid)
  );
  INSERT INTO type_pair_prior (src_type, dst_type, pid, n_obs)
  SELECT ds.type_qid, dd.type_qid, wl.prop, COUNT(*)
  FROM wd_links wl
  JOIN direct_types ds ON ds.qid = wl.src
  JOIN direct_types dd ON dd.qid = wl.dst
  GROUP BY 1,2,3;
  CREATE INDEX type_pair_prior_lookup_idx
    ON type_pair_prior(src_type, dst_type);
  RETURN (SELECT COUNT(*) FROM type_pair_prior);
END;
$$ LANGUAGE plpgsql;
