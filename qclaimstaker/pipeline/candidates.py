"""Thin wrappers that invoke the SQL refresh functions for §Candidate query,
§2 transitive closure, and §5 candidate-property generation."""

from __future__ import annotations

from ..config import settings
from ..db import connect


def refresh_transitive_paths() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_transitive_paths(%s, %s)::int AS n",
            (settings.transitive_pids, settings.transitive_max_depth),
        )
        n = cur.fetchone()["n"]
        conn.commit()
    return n


def refresh_candidate_pairs(dump_version: str) -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_candidate_pairs(%s, %s)::int AS n",
            (dump_version, settings.min_wp_count),
        )
        n = cur.fetchone()["n"]
        conn.commit()
    return n


def refresh_candidate_properties(dump_version: str) -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_candidate_properties(%s)::int AS n", (dump_version,)
        )
        n = cur.fetchone()["n"]
        conn.commit()
    return n


def refresh_type_pair_prior() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT refresh_type_pair_prior()::int AS n")
        n = cur.fetchone()["n"]
        conn.commit()
    return n
