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


-- §5: for each surviving pair, enumerate properties whose constraints match.
--
-- Same scaling problem as refresh_candidate_pairs: doing it as one big
-- INSERT...SELECT with seven correlated EXISTS clauses asks the planner to
-- evaluate ~1.2M candidates × ~12k constraints = 14B combinations, with a
-- type-closure walk inside each EXISTS. It spills terabytes.
--
-- Staging plan:
--   1. _useful_subj_types / _useful_val_types — types referenced anywhere in
--      property_constraints. ~10k rows each, vs the 9.3k of subclass_closure.
--   2. _src_match(qid, type_qid) — for each unique candidate src, the useful
--      ancestor types it has. Built once via direct_types ⋈ subclass_closure
--      restricted to candidate srcs and useful types. Same for _dst_match.
--   3. _src_pid(src, pid) — (src, pid) pairs where pid's subject_types is
--      satisfied by src (or pid has no subject_types). _dst_pid analogously.
--   4. Cross _src_pid ⋈ candidate_pairs ⋈ _dst_pid → seed _cprop. All joins
--      hit PK indexes, no nested correlated walks.
--   5. Drop on conflicts_with / requires / exceptions / one_of via small
--      flat tables. wd_links (src,dst,*) and inverse-coverage anti-joins are
--      omitted: stages B and C of refresh_candidate_pairs already enforced
--      that no wl(cp.src, cp.dst, *) exists and no wl(cp.dst, cp.src, prop_x)
--      exists for any prop_x with an inverse declared — making both checks
--      redundant here.
CREATE OR REPLACE FUNCTION refresh_candidate_properties(p_dump text)
RETURNS integer AS $$
DECLARE n integer;
BEGIN
  SET LOCAL work_mem = '2GB';

  DELETE FROM candidate_properties WHERE dump_version = p_dump;

  -- Stage 1: useful types referenced by any property_constraints column.
  CREATE TEMP TABLE _useful_subj_types ON COMMIT DROP AS
  SELECT DISTINCT t.type_qid
  FROM property_constraints pc, jsonb_array_elements_text(pc.subject_types) AS t(type_qid)
  WHERE pc.subject_types IS NOT NULL;
  CREATE INDEX ON _useful_subj_types(type_qid);
  ANALYZE _useful_subj_types;
  RAISE NOTICE '1. useful subject types: %', (SELECT COUNT(*) FROM _useful_subj_types);

  CREATE TEMP TABLE _useful_val_types ON COMMIT DROP AS
  SELECT DISTINCT t.type_qid
  FROM property_constraints pc, jsonb_array_elements_text(pc.value_types) AS t(type_qid)
  WHERE pc.value_types IS NOT NULL;
  CREATE INDEX ON _useful_val_types(type_qid);
  ANALYZE _useful_val_types;
  RAISE NOTICE '1. useful value types: %', (SELECT COUNT(*) FROM _useful_val_types);

  -- Stage 2: ancestor-type sets restricted to (candidate srcs/dsts, useful types).
  CREATE TEMP TABLE _src_match (qid text NOT NULL, type_qid text NOT NULL,
                                PRIMARY KEY (qid, type_qid)) ON COMMIT DROP;
  INSERT INTO _src_match
  SELECT DISTINCT cp.src, sc.super_qid
  FROM (SELECT DISTINCT src FROM candidate_pairs WHERE dump_version = p_dump) cp
  JOIN direct_types dt ON dt.qid = cp.src
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
  JOIN _useful_subj_types u ON u.type_qid = sc.super_qid;
  ANALYZE _src_match;
  RAISE NOTICE '2. src_match rows: %', (SELECT COUNT(*) FROM _src_match);

  CREATE TEMP TABLE _dst_match (qid text NOT NULL, type_qid text NOT NULL,
                                PRIMARY KEY (qid, type_qid)) ON COMMIT DROP;
  INSERT INTO _dst_match
  SELECT DISTINCT cp.dst, sc.super_qid
  FROM (SELECT DISTINCT dst FROM candidate_pairs WHERE dump_version = p_dump) cp
  JOIN direct_types dt ON dt.qid = cp.dst
  JOIN subclass_closure sc ON sc.sub_qid = dt.type_qid
  JOIN _useful_val_types u ON u.type_qid = sc.super_qid;
  ANALYZE _dst_match;
  RAISE NOTICE '2. dst_match rows: %', (SELECT COUNT(*) FROM _dst_match);

  -- Stage 3: collapse to (src, pid) and (dst, pid) compatibility tables.
  CREATE TEMP TABLE _src_pid (src text NOT NULL, pid text NOT NULL,
                              PRIMARY KEY (src, pid)) ON COMMIT DROP;
  -- (a) pids with subject-type constraint
  INSERT INTO _src_pid
  SELECT DISTINCT sm.qid, pc.pid
  FROM property_constraints pc
  JOIN jsonb_array_elements_text(pc.subject_types) AS st(type_qid) ON TRUE
  JOIN _src_match sm ON sm.type_qid = st.type_qid
  WHERE pc.subject_types IS NOT NULL
  ON CONFLICT DO NOTHING;
  -- (b) pids with NO subject-type constraint: every candidate src qualifies
  INSERT INTO _src_pid
  SELECT cs.src, pc.pid
  FROM (SELECT DISTINCT src FROM candidate_pairs WHERE dump_version = p_dump) cs
  CROSS JOIN property_constraints pc
  WHERE pc.subject_types IS NULL
  ON CONFLICT DO NOTHING;
  ANALYZE _src_pid;
  RAISE NOTICE '3. src_pid rows: %', (SELECT COUNT(*) FROM _src_pid);

  CREATE TEMP TABLE _dst_pid (dst text NOT NULL, pid text NOT NULL,
                              PRIMARY KEY (dst, pid)) ON COMMIT DROP;
  INSERT INTO _dst_pid
  SELECT DISTINCT dm.qid, pc.pid
  FROM property_constraints pc
  JOIN jsonb_array_elements_text(pc.value_types) AS vt(type_qid) ON TRUE
  JOIN _dst_match dm ON dm.type_qid = vt.type_qid
  WHERE pc.value_types IS NOT NULL
  ON CONFLICT DO NOTHING;
  INSERT INTO _dst_pid
  SELECT cs.dst, pc.pid
  FROM (SELECT DISTINCT dst FROM candidate_pairs WHERE dump_version = p_dump) cs
  CROSS JOIN property_constraints pc
  WHERE pc.value_types IS NULL
  ON CONFLICT DO NOTHING;
  ANALYZE _dst_pid;
  RAISE NOTICE '3. dst_pid rows: %', (SELECT COUNT(*) FROM _dst_pid);

  -- Stage 4: seed _cprop via three-way PK join.
  CREATE TEMP TABLE _cprop (
    src text NOT NULL,
    dst text NOT NULL,
    pid text NOT NULL,
    PRIMARY KEY (src, dst, pid)
  ) ON COMMIT DROP;
  INSERT INTO _cprop (src, dst, pid)
  SELECT cp.src, cp.dst, sp.pid
  FROM candidate_pairs cp
  JOIN _src_pid sp ON sp.src = cp.src
  JOIN _dst_pid dp ON dp.dst = cp.dst AND dp.pid = sp.pid
  WHERE cp.dump_version = p_dump;
  ANALYZE _cprop;
  RAISE NOTICE '4. after subject/value match: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 5: one_of — flatten then anti-join.
  CREATE TEMP TABLE _pc_one_of (pid text NOT NULL, qid text NOT NULL) ON COMMIT DROP;
  INSERT INTO _pc_one_of
  SELECT pc.pid, o.qid
  FROM property_constraints pc
  JOIN jsonb_array_elements_text(pc.one_of) o(qid) ON TRUE
  WHERE pc.one_of IS NOT NULL;
  CREATE INDEX ON _pc_one_of(pid, qid);
  -- delete _cprop rows whose pid declares one_of but cp.dst is not in it
  DELETE FROM _cprop cp
  WHERE EXISTS (SELECT 1 FROM _pc_one_of o WHERE o.pid = cp.pid)
    AND NOT EXISTS (SELECT 1 FROM _pc_one_of o WHERE o.pid = cp.pid AND o.qid = cp.dst);
  RAISE NOTICE '5. after one_of: %', (SELECT COUNT(*) FROM _cprop);

  -- Stage 6: exceptions — small flat table.
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

  -- Stage 7: conflicts_with — flatten (pid, conflict_pid), then drop pairs
  -- where wl(src, conflict_pid, *) exists.
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
