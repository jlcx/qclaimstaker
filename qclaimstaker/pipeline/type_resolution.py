"""Populate direct_types from wd_links (prop='P31') and refresh subclass_closure
and type_closure via the SQL functions.

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


def refresh_closures() -> tuple[int, int]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_subclass_closure(%s)::int AS n", (settings.subclass_max_depth,)
        )
        sc_n = cur.fetchone()["n"]
        cur.execute("SELECT refresh_type_closure()::int AS n")
        tc_n = cur.fetchone()["n"]
        conn.commit()
    return sc_n, tc_n
