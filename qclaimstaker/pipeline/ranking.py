"""Score (src, dst, pid) candidates per SPEC §6 and write top-k to proposals.

Score components (weights live in Settings):
  - prior:     max over (s_type, d_type) ∈ direct_types(src) × direct_types(dst)
               of Laplace-smoothed P(pid | s_type, d_type).
  - specificity: 1 if any direct type of src matches a declared subject type
                 of pid (or pid has no subject constraint), else 0; +1 same
                 for dst/value.
  - usage_penalty: normalized log(usage_count).
  - evidence: normalized log(wp_count).
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Iterable

from ..config import settings
from ..db import connect


def _direct_types(cur, qid: str) -> set[str]:
    cur.execute("SELECT type_qid FROM direct_types WHERE qid = %s", (qid,))
    return {r["type_qid"] for r in cur.fetchall()}


def _prior_lookup(
    cur,
    src_types: set[str],
    dst_types: set[str],
    pids: Iterable[str],
    k_pids: int,
) -> dict[str, float]:
    """For each pid, max-over-(s,d)-type-pair smoothed prior."""
    if not src_types or not dst_types or not pids:
        return {p: 0.0 for p in pids}
    alpha = settings.smoothing_alpha
    pids_list = list(pids)
    cur.execute(
        """
        SELECT src_type, dst_type, pid, n_obs
        FROM type_pair_prior
        WHERE src_type = ANY(%s) AND dst_type = ANY(%s) AND pid = ANY(%s)
        """,
        (list(src_types), list(dst_types), pids_list),
    )
    per_tp_n = defaultdict(lambda: defaultdict(int))  # (s,d) -> pid -> n
    for r in cur.fetchall():
        per_tp_n[(r["src_type"], r["dst_type"])][r["pid"]] = r["n_obs"]

    cur.execute(
        """
        SELECT src_type, dst_type, SUM(n_obs)::bigint AS total
        FROM type_pair_prior
        WHERE src_type = ANY(%s) AND dst_type = ANY(%s)
        GROUP BY src_type, dst_type
        """,
        (list(src_types), list(dst_types)),
    )
    totals = {(r["src_type"], r["dst_type"]): r["total"] for r in cur.fetchall()}

    out: dict[str, float] = {}
    for pid in pids_list:
        best = 0.0
        for s in src_types:
            for d in dst_types:
                n = per_tp_n.get((s, d), {}).get(pid, 0)
                total = totals.get((s, d), 0)
                p = (n + alpha) / (total + alpha * max(k_pids, 1))
                if p > best:
                    best = p
        out[pid] = best
    return out


def _specificity(
    cur, src: str, dst: str, constraints: dict[str, tuple[set[str], set[str]]]
) -> dict[str, float]:
    """Direct-type matches (score 1) vs. only-via-closure matches (score 0)."""
    src_direct = _direct_types(cur, src)
    dst_direct = _direct_types(cur, dst)
    out: dict[str, float] = {}
    for pid, (s_types, v_types) in constraints.items():
        s_spec = 1.0 if (not s_types) or (src_direct & s_types) else 0.0
        v_spec = 1.0 if (not v_types) or (dst_direct & v_types) else 0.0
        out[pid] = s_spec + v_spec
    return out


def _normalize_log(x: float, cap: float) -> float:
    if x <= 0:
        return 0.0
    return min(math.log(1 + x) / math.log(1 + cap), 1.0)


def rank_pair(
    cur,
    src: str,
    dst: str,
    wp_count: int,
    pids: list[str],
) -> list[tuple[str, float]]:
    """Return [(pid, score), ...] sorted desc, for all candidate pids."""
    if not pids:
        return []

    # pull constraints for specificity + usage
    cur.execute(
        "SELECT pid, subject_types, value_types, usage_count "
        "FROM property_constraints WHERE pid = ANY(%s)",
        (pids,),
    )
    constraints: dict[str, tuple[set[str], set[str]]] = {}
    usage: dict[str, int] = {}
    for r in cur.fetchall():
        s_types = set(r["subject_types"] or [])
        v_types = set(r["value_types"] or [])
        constraints[r["pid"]] = (s_types, v_types)
        usage[r["pid"]] = int(r["usage_count"] or 0)

    # number of distinct pids in the universe — used for additive smoothing K.
    cur.execute("SELECT COUNT(DISTINCT pid)::int AS n FROM type_pair_prior")
    k_pids = cur.fetchone()["n"] or 1

    src_types = _direct_types(cur, src)
    dst_types = _direct_types(cur, dst)
    prior = _prior_lookup(cur, src_types, dst_types, pids, k_pids)
    spec = _specificity(cur, src, dst, constraints)

    # normalize usage penalty by the global max usage
    max_usage = max(usage.values(), default=1)
    evidence = _normalize_log(wp_count, cap=10_000)

    scored: list[tuple[str, float]] = []
    for pid in pids:
        u = _normalize_log(usage.get(pid, 0), cap=max(max_usage, 1))
        score = (
            settings.w_prior * prior.get(pid, 0.0)
            + settings.w_specificity * (spec.get(pid, 0.0) / 2.0)
            - settings.w_usage_penalty * u
            + settings.w_evidence * evidence
        )
        scored.append((pid, score))
    scored.sort(key=lambda p: p[1], reverse=True)
    return scored


def iter_pairs(dump_version: str):
    """Stream candidate_pairs + their candidate pids grouped per pair."""
    with connect() as conn:
        with conn.cursor(name="pair_stream") as read:
            read.itersize = 2000
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
                yield row["src"], row["dst"], row["wp_count"], [
                    p for p in (row["pids"] or []) if p is not None
                ]
