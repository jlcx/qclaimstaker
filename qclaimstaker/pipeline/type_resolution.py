"""Populate direct_types from wd_links (prop='P31'), cache P279 edges, then
refresh subclass_closure and type_closure via the SQL functions.

Item JSON is not required — P31 is captured in wd_links. Only wd_properties
carries full JSON (for P2302 constraint qualifiers)."""

from __future__ import annotations

from ..config import settings
from ..db import connect


def refresh_direct_types() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE direct_types")
        cur.execute(
            """
            INSERT INTO direct_types (qid, type_qid)
            SELECT DISTINCT src, dst
            FROM wd_links
            WHERE prop = 'P31'
            """
        )
        n = cur.rowcount
        conn.commit()
    return n


def refresh_p279_edges() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT refresh_p279_edges()::int AS n")
        n = cur.fetchone()["n"]
        conn.commit()
    return n


def refresh_closures() -> tuple[int, int]:
    """Refresh subclass_closure. type_closure is not materialized at Wikidata
    scale (see 010_subclass_closure.sql); returns (sc_rows, 0)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_subclass_closure(%s)::int AS n", (settings.subclass_max_depth,)
        )
        sc_n = cur.fetchone()["n"]
        conn.commit()
    return sc_n, 0
