"""Rank pairs, assign tiers, write proposals + rejected_pairs (§7)."""

from __future__ import annotations

from ..config import settings
from ..db import connect
from .ranking import iter_pairs, rank_pair


def _tier(top_score: float, margin: float, top_pid: str) -> str:
    if top_pid in settings.always_review:
        return "review_queue"
    if (
        top_score >= settings.auto_score_threshold
        and margin >= settings.auto_margin_threshold
    ):
        return "auto_queue"
    return "review_queue"


def rank_and_tier(dump_version: str) -> dict[str, int]:
    counts = {"auto_queue": 0, "review_queue": 0, "rejected_auto": 0}
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM proposals WHERE dump_version = %s", (dump_version,))
            cur.execute(
                "DELETE FROM rejected_pairs WHERE dump_version = %s", (dump_version,)
            )
        with conn.cursor() as write, conn.cursor() as inner:
            for src, dst, wp_count, pids in iter_pairs(dump_version):
                if not pids:
                    write.execute(
                        "INSERT INTO rejected_pairs (src, dst, reason, dump_version) "
                        "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (src, dst, "no property survived §5 constraints", dump_version),
                    )
                    counts["rejected_auto"] += 1
                    continue
                scored = rank_pair(inner, src, dst, wp_count, pids)
                top = scored[: settings.top_k]
                top_score = top[0][1]
                next_score = top[1][1] if len(top) > 1 else 0.0
                margin = top_score - next_score
                tier = _tier(top_score, margin, top[0][0])
                counts[tier] += 1

                # Look up direction per pid (inverse properties can be presented in
                # their natural direction by the UI / emitter).
                inner.execute(
                    "SELECT pid, direction FROM candidate_properties "
                    "WHERE src=%s AND dst=%s AND dump_version=%s",
                    (src, dst, dump_version),
                )
                direction = {r["pid"]: r["direction"] for r in inner.fetchall()}

                for rank, (pid, score) in enumerate(top, start=1):
                    m = score - (top[rank][1] if rank < len(top) else 0.0)
                    write.execute(
                        """
                        INSERT INTO proposals
                          (src, dst, pid, score, margin, tier, direction,
                           rank, wp_count, dump_version)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (src, dst, pid, dump_version) DO UPDATE
                          SET score = EXCLUDED.score,
                              margin = EXCLUDED.margin,
                              tier = EXCLUDED.tier,
                              direction = EXCLUDED.direction,
                              rank = EXCLUDED.rank,
                              wp_count = EXCLUDED.wp_count,
                              created_at = now()
                        """,
                        (
                            src, dst, pid, score, m,
                            tier if rank == 1 else "review_queue",
                            direction.get(pid, "forward"),
                            rank, wp_count, dump_version,
                        ),
                    )
        conn.commit()
    return counts
