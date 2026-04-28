-- §Candidate query + §5 Property candidate generation, both in SQL.

-- Staged with checkpoints. Doing all seven anti-joins in one CTE confuses
-- the planner at Wikidata scale — especially the inverse-normalized
-- coverage step, which used to UNION all 900M rows of wd_links and dedup.
-- Splitting into stages lets each anti-join pick its own plan, gives us
-- progress visibility, and lets the temp table shrink before each
-- successive (more expensive) stage.

CREATE OR REPLACE FUNCTION refresh_candidate_pairs(
  p_dump text,
  p_min_wp_count int DEFAULT 20
)
RETURNS integer AS $$
DECLARE
  n integer;
BEGIN
  SET LOCAL work_mem = '2GB';

  DELETE FROM candidate_pairs WHERE dump_version = p_dump;
  DELETE FROM rejected_pairs  WHERE dump_version = p_dump;

  CREATE TEMP TABLE _cp (
    src text NOT NULL,
    dst text NOT NULL,
    wp_count integer NOT NULL,
    PRIMARY KEY (src, dst)
  ) ON COMMIT DROP;

  -- Stage A: wp_count threshold.
  INSERT INTO _cp (src, dst, wp_count)
  SELECT src, dst, wp_count
  FROM wp_links
  WHERE wp_count >= p_min_wp_count
  ON CONFLICT DO NOTHING;
  ANALYZE _cp;
  RAISE NOTICE 'A. wp_count >= %: % pairs', p_min_wp_count, (SELECT COUNT(*) FROM _cp);

  -- Stage B: drop pairs already linked in wd_links (forward direction).
  -- wd_links PK is (src, dst, prop) so (src, dst) probes are index-prefix.
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1 FROM wd_links wl WHERE wl.src = c.src AND wl.dst = c.dst);
  RAISE NOTICE 'B. after wd_links forward dedup: %', (SELECT COUNT(*) FROM _cp);

  -- Stage C: drop pairs whose reverse exists under an inverse PID.
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1 FROM wd_links wl
    JOIN inverse_properties ip ON ip.pid_a = wl.prop
    WHERE wl.src = c.dst AND wl.dst = c.src);
  RAISE NOTICE 'C. after inverse-coverage dedup: %', (SELECT COUNT(*) FROM _cp);

  -- Stage D: drop pairs already in transitive_paths.
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1 FROM transitive_paths t WHERE t.src = c.src AND t.dst = c.dst);
  RAISE NOTICE 'D. after transitive_paths: %', (SELECT COUNT(*) FROM _cp);

  -- Stage E: P31 ∘ P279* composition. dst is already an ancestor-type of
  -- src's direct type(s). Inlined rather than folded into transitive_paths
  -- because materializing that join at scale spills terabytes.
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1
    FROM direct_types dt
    JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
    WHERE dt.qid = c.src AND sc.super_qid = c.dst AND sc.depth > 0);
  RAISE NOTICE 'E. after P31 composed-with P279*: %', (SELECT COUNT(*) FROM _cp);

  -- Stage F: meta_type_blocklist on either side (via type_closure inlined).
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1 FROM meta_type_blocklist m
    JOIN direct_types dt ON dt.qid = c.src OR dt.qid = c.dst
    JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
                            AND sc.super_qid = m.qid);
  RAISE NOTICE 'F. after meta_type_blocklist: %', (SELECT COUNT(*) FROM _cp);

  -- Stage G: source/destination blocklists (direct qid match).
  DELETE FROM _cp c
  WHERE EXISTS (SELECT 1 FROM source_blocklist sb      WHERE sb.qid = c.src)
     OR EXISTS (SELECT 1 FROM destination_blocklist db WHERE db.qid = c.dst);
  RAISE NOTICE 'G. after src/dst blocklist: %', (SELECT COUNT(*) FROM _cp);

  -- Stage H: type-assertion dedup (one side is already a declared type of
  -- the other, transitively).
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1
    FROM direct_types dt
    JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
    WHERE dt.qid = c.src AND sc.super_qid = c.dst);
  DELETE FROM _cp c WHERE EXISTS (
    SELECT 1
    FROM direct_types dt
    JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
    WHERE dt.qid = c.dst AND sc.super_qid = c.src);
  RAISE NOTICE 'H. after type-assertion dedup: %', (SELECT COUNT(*) FROM _cp);

  INSERT INTO candidate_pairs (src, dst, wp_count, dump_version)
  SELECT src, dst, wp_count, p_dump FROM _cp;

  GET DIAGNOSTICS n = ROW_COUNT;
  RAISE NOTICE 'candidate_pairs(% ) for %: % rows', p_dump, p_dump, n;
  RETURN n;
END;
$$ LANGUAGE plpgsql;


-- §5: for each surviving pair, enumerate properties whose constraints match
-- AND that have empirical evidence between the pair's direct types.
--
-- The constraint-only seed (subject_types ∪ value_types compatibility) is
-- combinatorially explosive: common types like Q5 (human) appear in
-- hundreds of properties' subject_types, so every human-src pair gets
-- ~3000 compatible pids before any meaningful filter runs. At 614k pairs
-- that's ~1B intermediate triples and >250 GB temp.
--
-- Instead, drive the seed from `type_pair_prior(src_type, dst_type, pid)`:
-- only consider (src, dst, pid) where some (direct_type(src),
-- direct_type(dst)) combination has at least one observation of pid in
-- wd_links. That naturally caps the per-pair candidate set to pids with
-- empirical support — typically tens, not thousands. Constraint
-- type-compat then runs as a *refinement* filter against the much smaller
-- surviving set. type_pair_prior is independent of candidate_pairs (it's
-- a pure aggregation of wd_links by direct types) so it can be built
-- once per dump and reused.
--
-- Coverage tradeoff: pids never previously observed for any type
-- combination of the pair are excluded. The spec's ranking already
-- assigns near-zero score to those via the type-pair prior, so the loss
-- is a small recall cost on long-tail propositions in exchange for
-- making the function tractable at full Wikidata scale.
CREATE OR REPLACE FUNCTION refresh_candidate_properties(p_dump text)
RETURNS integer AS $$
DECLARE n integer;
BEGIN
  SET LOCAL work_mem = '2GB';

  IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_name = 'type_pair_prior') THEN
    RAISE EXCEPTION 'type_pair_prior must exist before refresh_candidate_properties; '
                    'run refresh_type_pair_prior() first (qclaimstaker refresh-prior)';
  END IF;

  DELETE FROM candidate_properties WHERE dump_version = p_dump;

  -- Stage 1: seed _cprop with (src, dst, pid) where pid has empirical evidence
  -- between some direct type of src and some direct type of dst.
  CREATE TEMP TABLE _cprop (
    src text NOT NULL,
    dst text NOT NULL,
    pid text NOT NULL,
    PRIMARY KEY (src, dst, pid)
  ) ON COMMIT DROP;
  INSERT INTO _cprop (src, dst, pid)
  SELECT DISTINCT cp.src, cp.dst, tpp.pid
  FROM candidate_pairs cp
  JOIN direct_types ds ON ds.qid = cp.src
  JOIN direct_types dd ON dd.qid = cp.dst
  JOIN type_pair_prior tpp
    ON tpp.src_type = ds.type_qid AND tpp.dst_type = dd.type_qid
  WHERE cp.dump_version = p_dump
  ON CONFLICT DO NOTHING;
  ANALYZE _cprop;
  RAISE NOTICE '1. _cprop seeded by type_pair_prior: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 2: per-pid constraint-type tables, restricted to pids appearing in
  -- _cprop. Then ancestor-type matches for srcs/dsts in _cprop, restricted
  -- to types those pids actually mention. Both restrictions cut the
  -- intermediate sizes by 10–100x vs the unrestricted versions.
  CREATE TEMP TABLE _pc_subj (pid text NOT NULL, type_qid text NOT NULL,
                              PRIMARY KEY (pid, type_qid)) ON COMMIT DROP;
  INSERT INTO _pc_subj
  SELECT DISTINCT pc.pid, st.type_qid
  FROM property_constraints pc
  JOIN (SELECT DISTINCT pid FROM _cprop) cs ON cs.pid = pc.pid
  JOIN jsonb_array_elements_text(pc.subject_types) AS st(type_qid) ON TRUE
  WHERE pc.subject_types IS NOT NULL
  ON CONFLICT DO NOTHING;
  CREATE INDEX ON _pc_subj(type_qid);
  ANALYZE _pc_subj;

  CREATE TEMP TABLE _pc_val (pid text NOT NULL, type_qid text NOT NULL,
                             PRIMARY KEY (pid, type_qid)) ON COMMIT DROP;
  INSERT INTO _pc_val
  SELECT DISTINCT pc.pid, vt.type_qid
  FROM property_constraints pc
  JOIN (SELECT DISTINCT pid FROM _cprop) cs ON cs.pid = pc.pid
  JOIN jsonb_array_elements_text(pc.value_types) AS vt(type_qid) ON TRUE
  WHERE pc.value_types IS NOT NULL
  ON CONFLICT DO NOTHING;
  CREATE INDEX ON _pc_val(type_qid);
  ANALYZE _pc_val;
  RAISE NOTICE '2. _pc_subj=%, _pc_val=%',
    (SELECT COUNT(*) FROM _pc_subj), (SELECT COUNT(*) FROM _pc_val);

  CREATE TEMP TABLE _src_match (qid text NOT NULL, type_qid text NOT NULL,
                                PRIMARY KEY (qid, type_qid)) ON COMMIT DROP;
  INSERT INTO _src_match
  SELECT DISTINCT cs.src, sc.super_qid
  FROM (SELECT DISTINCT src FROM _cprop) cs
  JOIN direct_types dt ON dt.qid = cs.src
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
  JOIN _pc_subj u ON u.type_qid = sc.super_qid;
  ANALYZE _src_match;

  CREATE TEMP TABLE _dst_match (qid text NOT NULL, type_qid text NOT NULL,
                                PRIMARY KEY (qid, type_qid)) ON COMMIT DROP;
  INSERT INTO _dst_match
  SELECT DISTINCT cs.dst, sc.super_qid
  FROM (SELECT DISTINCT dst FROM _cprop) cs
  JOIN direct_types dt ON dt.qid = cs.dst
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
  JOIN _pc_val u ON u.type_qid = sc.super_qid;
  ANALYZE _dst_match;
  RAISE NOTICE '2. _src_match=%, _dst_match=%',
    (SELECT COUNT(*) FROM _src_match), (SELECT COUNT(*) FROM _dst_match);

  -- Stage 3: drop _cprop rows where pid declares subject_types and src has no
  -- ancestor type in that set. Pids without subject_types are universally
  -- compatible and remain.
  DELETE FROM _cprop cp
  WHERE EXISTS (SELECT 1 FROM _pc_subj WHERE pid = cp.pid)
    AND NOT EXISTS (
      SELECT 1 FROM _pc_subj ps
      JOIN _src_match sm ON sm.qid = cp.src AND sm.type_qid = ps.type_qid
      WHERE ps.pid = cp.pid);
  RAISE NOTICE '3. after subject_types filter: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 4: same for value_types.
  DELETE FROM _cprop cp
  WHERE EXISTS (SELECT 1 FROM _pc_val WHERE pid = cp.pid)
    AND NOT EXISTS (
      SELECT 1 FROM _pc_val pv
      JOIN _dst_match dm ON dm.qid = cp.dst AND dm.type_qid = pv.type_qid
      WHERE pv.pid = cp.pid);
  RAISE NOTICE '4. after value_types filter: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 5: one_of.
  CREATE TEMP TABLE _pc_one_of (pid text NOT NULL, qid text NOT NULL) ON COMMIT DROP;
  INSERT INTO _pc_one_of
  SELECT pc.pid, o.qid
  FROM property_constraints pc
  JOIN jsonb_array_elements_text(pc.one_of) o(qid) ON TRUE
  WHERE pc.one_of IS NOT NULL;
  CREATE INDEX ON _pc_one_of(pid, qid);
  DELETE FROM _cprop cp
  WHERE EXISTS (SELECT 1 FROM _pc_one_of o WHERE o.pid = cp.pid)
    AND NOT EXISTS (SELECT 1 FROM _pc_one_of o WHERE o.pid = cp.pid AND o.qid = cp.dst);
  RAISE NOTICE '5. after one_of: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 6: exceptions.
  CREATE TEMP TABLE _pc_exc (pid text NOT NULL, qid text NOT NULL,
                             PRIMARY KEY (pid, qid)) ON COMMIT DROP;
  INSERT INTO _pc_exc
  SELECT pc.pid, e.qid
  FROM property_constraints pc
  JOIN jsonb_array_elements_text(pc.exceptions) e(qid) ON TRUE
  WHERE pc.exceptions IS NOT NULL
  ON CONFLICT DO NOTHING;
  DELETE FROM _cprop cp WHERE EXISTS (
    SELECT 1 FROM _pc_exc x WHERE x.pid = cp.pid AND (x.qid = cp.src OR x.qid = cp.dst));
  RAISE NOTICE '6. after exceptions: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 7: conflicts_with — drop pairs where wl(src, conflict_pid, *) exists.
  CREATE TEMP TABLE _pc_conflict (pid text NOT NULL, conflict_pid text NOT NULL,
                                  PRIMARY KEY (pid, conflict_pid)) ON COMMIT DROP;
  INSERT INTO _pc_conflict
  SELECT pc.pid, (c.c->>'pid')
  FROM property_constraints pc
  JOIN jsonb_array_elements(pc.conflicts_with) c(c) ON TRUE
  WHERE pc.conflicts_with IS NOT NULL
  ON CONFLICT DO NOTHING;
  CREATE INDEX ON _pc_conflict(pid);
  ANALYZE _pc_conflict;
  DELETE FROM _cprop cp WHERE EXISTS (
    SELECT 1 FROM _pc_conflict pcc
    JOIN wd_links wl ON wl.src = cp.src AND wl.prop = pcc.conflict_pid
    WHERE pcc.pid = cp.pid);
  RAISE NOTICE '7. after conflicts_with: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 8: requires — pid requires that wl(cp.src, req_pid, *) exists.
  CREATE TEMP TABLE _pc_req (pid text NOT NULL, req_pid text NOT NULL,
                             PRIMARY KEY (pid, req_pid)) ON COMMIT DROP;
  INSERT INTO _pc_req
  SELECT pc.pid, (r.r->>'pid')
  FROM property_constraints pc
  JOIN jsonb_array_elements(pc.requires) r(r) ON TRUE
  WHERE pc.requires IS NOT NULL
  ON CONFLICT DO NOTHING;
  CREATE INDEX ON _pc_req(pid);
  ANALYZE _pc_req;
  DELETE FROM _cprop cp WHERE EXISTS (
    SELECT 1 FROM _pc_req pcr
    WHERE pcr.pid = cp.pid
      AND NOT EXISTS (
        SELECT 1 FROM wd_links wl
        WHERE wl.src = cp.src AND wl.prop = pcr.req_pid));
  RAISE NOTICE '8. after requires: %', (SELECT COUNT(*) FROM _cprop);

  INSERT INTO candidate_properties (src, dst, pid, direction, dump_version)
  SELECT src, dst, pid, 'forward', p_dump FROM _cprop;

  GET DIAGNOSTICS n = ROW_COUNT;
  RAISE NOTICE 'candidate_properties for %: % rows', p_dump, n;
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
