"""Score (src, dst, pid) candidates per SPEC §6. Pure Python — all SQL reads
happen upfront in `tiering.rank_and_tier`.

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

from ..config import settings


def _normalize_log(x: float, cap: float) -> float:
    if x <= 0:
        return 0.0
    return min(math.log(1 + x) / math.log(1 + cap), 1.0)


def score_pids(
    src_types: set[str],
    dst_types: set[str],
    wp_count: int,
    pids: list[str],
    constraints: dict[str, tuple[set[str], set[str]]],
    usage: dict[str, int],
    global_max_usage: int,
    tpp_n: dict[tuple[str, str], dict[str, int]],
    tpp_total: dict[tuple[str, str], int],
    k_pids: int,
) -> list[tuple[str, float]]:
    """Return [(pid, score), ...] sorted desc."""
    alpha = settings.smoothing_alpha
    evidence = _normalize_log(wp_count, cap=10_000)
    cap_usage = max(global_max_usage, 1)

    scored: list[tuple[str, float]] = []
    for pid in pids:
        s_types, v_types = constraints.get(pid, (set(), set()))
        s_spec = 1.0 if (not s_types) or (src_types & s_types) else 0.0
        v_spec = 1.0 if (not v_types) or (dst_types & v_types) else 0.0
        spec = s_spec + v_spec

        best_prior = 0.0
        for s in src_types:
            for d in dst_types:
                k = (s, d)
                n = tpp_n.get(k, {}).get(pid, 0)
                total = tpp_total.get(k, 0)
                p = (n + alpha) / (total + alpha * k_pids)
                if p > best_prior:
                    best_prior = p

        u = _normalize_log(usage.get(pid, 0), cap=cap_usage)
        score = (
            settings.w_prior * best_prior
            + settings.w_specificity * (spec / 2.0)
            - settings.w_usage_penalty * u
            + settings.w_evidence * evidence
        )
        scored.append((pid, score))

    scored.sort(key=lambda p: p[1], reverse=True)
    return scored
