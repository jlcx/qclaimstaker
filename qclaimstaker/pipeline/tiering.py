"""Rank pairs, assign tiers, write proposals + rejected_pairs (§7).

All SQL reads happen upfront in bulk; the per-pair loop is pure Python
arithmetic plus buffered INSERT batches. The previous per-pair-query
implementation was O(N_pairs × constants) and ran for days at full
Wikidata scale because each pair re-ran a 10M-row COUNT(DISTINCT pid)
on type_pair_prior."""

from __future__ import annotations

import sys
from collections import defaultdict

from ..config import settings
from ..db import connect
from .ranking import score_pids


_BATCH = 5000


def _tier(top_score: float, margin: float, top_pid: str) -> str:
    if top_pid in settings.always_review:
        return "review_queue"
    if (
        top_score >= settings.auto_score_threshold
        and margin >= settings.auto_margin_threshold
    ):
        return "auto_queue"
    return "review_queue"


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def rank_and_tier(dump_version: str) -> dict[str, int]:
    counts = {"auto_queue": 0, "review_queue": 0, "rejected_auto": 0}

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM proposals WHERE dump_version = %s", (dump_version,))
            cur.execute(
                "DELETE FROM rejected_pairs WHERE dump_version = %s", (dump_version,)
            )

            _log("loading constants...")
            cur.execute("SELECT COUNT(DISTINCT pid)::int AS n FROM type_pair_prior")
            k_pids = cur.fetchone()["n"] or 1

            cur.execute(
                "SELECT pid, subject_types, value_types, usage_count "
                "FROM property_constraints"
            )
            constraints: dict[str, tuple[set[str], set[str]]] = {}
            usage: dict[str, int] = {}
            for r in cur.fetchall():
                constraints[r["pid"]] = (
                    set(r["subject_types"] or []),
                    set(r["value_types"] or []),
                )
                usage[r["pid"]] = int(r["usage_count"] or 0)
            global_max_usage = max(usage.values(), default=1)
            _log(
                f"  k_pids={k_pids}, constraints={len(constraints)}, "
                f"max_usage={global_max_usage}"
            )

            _log("loading direct_types for candidate srcs/dsts...")
            cur.execute(
                """
                WITH cq AS (
                    SELECT src AS qid FROM candidate_pairs WHERE dump_version = %s
                    UNION
                    SELECT dst AS qid FROM candidate_pairs WHERE dump_version = %s
                )
                SELECT dt.qid, dt.type_qid
                FROM direct_types dt
                JOIN cq ON cq.qid = dt.qid
                """,
                (dump_version, dump_version),
            )
            direct_types: dict[str, set[str]] = defaultdict(set)
            for r in cur.fetchall():
                direct_types[r["qid"]].add(r["type_qid"])
            _log(f"  direct_types entries: {len(direct_types)}")

            _log("loading type_pair_prior slice...")
            cur.execute(
                """
                WITH pair_types AS (
                    SELECT DISTINCT ds.type_qid AS s_type, dd.type_qid AS d_type
                    FROM candidate_pairs cp
                    JOIN direct_types ds ON ds.qid = cp.src
                    JOIN direct_types dd ON dd.qid = cp.dst
                    WHERE cp.dump_version = %s
                )
                SELECT tpp.src_type, tpp.dst_type, tpp.pid, tpp.n_obs
                FROM type_pair_prior tpp
                JOIN pair_types pt
                  ON pt.s_type = tpp.src_type AND pt.d_type = tpp.dst_type
                """,
                (dump_version,),
            )
            tpp_n: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
            tpp_total: dict[tuple[str, str], int] = defaultdict(int)
            for r in cur.fetchall():
                key = (r["src_type"], r["dst_type"])
                tpp_n[key][r["pid"]] = r["n_obs"]
                tpp_total[key] += r["n_obs"]
            _log(f"  type_pair_prior entries: {sum(len(v) for v in tpp_n.values())}")

            _log("loading directions...")
            cur.execute(
                "SELECT src, dst, pid, direction FROM candidate_properties "
                "WHERE dump_version = %s",
                (dump_version,),
            )
            direction: dict[tuple[str, str, str], str] = {}
            for r in cur.fetchall():
                direction[(r["src"], r["dst"], r["pid"])] = r["direction"]
            _log(f"  directions: {len(direction)}")

        rejects_buffer: list[tuple] = []
        proposals_buffer: list[tuple] = []
        n_seen = 0

        _log("scoring pairs...")
        with conn.cursor(name="pair_stream") as read:
            read.itersize = 5000
            read.execute(
                """
                SELECT cp.src, cp.dst, cp.wp_count,
                       array_agg(pr.pid ORDER BY pr.pid) AS pids
                FROM candidate_pairs cp
                LEFT JOIN candidate_properties pr
                  ON pr.src = cp.src AND pr.dst = cp.dst
                 AND pr.dump_version = cp.dump_version
                WHERE cp.dump_version = %s
                GROUP BY cp.src, cp.dst, cp.wp_count
                """,
                (dump_version,),
            )

            for row in read:
                n_seen += 1
                if n_seen % 50_000 == 0:
                    _log(f"  scored {n_seen} pairs")

                src = row["src"]
                dst = row["dst"]
                wp_count = row["wp_count"]
                pids = [p for p in (row["pids"] or []) if p is not None]

                if not pids:
                    rejects_buffer.append(
                        (src, dst, "no property survived §5 constraints", dump_version)
                    )
                    counts["rejected_auto"] += 1
                    if len(rejects_buffer) >= _BATCH:
                        _flush_rejects(conn, rejects_buffer)
                        rejects_buffer = []
                    continue

                src_types = direct_types.get(src, set())
                dst_types = direct_types.get(dst, set())

                scored = score_pids(
                    src_types,
                    dst_types,
                    wp_count,
                    pids,
                    constraints,
                    usage,
                    global_max_usage,
                    tpp_n,
                    tpp_total,
                    k_pids,
                )

                top = scored[: settings.top_k]
                top_score = top[0][1]
                next_score = top[1][1] if len(top) > 1 else 0.0
                margin = top_score - next_score
                tier = _tier(top_score, margin, top[0][0])
                counts[tier] += 1

                for rank, (pid, score) in enumerate(top, start=1):
                    m = score - (top[rank][1] if rank < len(top) else 0.0)
                    proposals_buffer.append(
                        (
                            src, dst, pid, score, m,
                            tier if rank == 1 else "review_queue",
                            direction.get((src, dst, pid), "forward"),
                            rank, wp_count, dump_version,
                        )
                    )

                if len(proposals_buffer) >= _BATCH:
                    _flush_proposals(conn, proposals_buffer)
                    proposals_buffer = []

        if rejects_buffer:
            _flush_rejects(conn, rejects_buffer)
        if proposals_buffer:
            _flush_proposals(conn, proposals_buffer)
        conn.commit()

    _log(f"done: {n_seen} pairs, counts={counts}")
    return counts


def _flush_rejects(conn, buf: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO rejected_pairs (src, dst, reason, dump_version) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            buf,
        )


def _flush_proposals(conn, buf: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO proposals
              (src, dst, pid, score, margin, tier, direction,
               rank, wp_count, dump_version)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            buf,
        )
