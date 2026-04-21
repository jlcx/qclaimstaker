"""Harvest inverse-of (P1696) claims from wd_properties, append to the
hand-curated seed loaded by 003_seed_inverses.sql. Both directions are
inserted so lookups are symmetric."""

from __future__ import annotations

from typing import Any, Iterator

from ..db import connect


def _iter_p1696(claims: dict[str, Any] | None) -> Iterator[str]:
    if not claims:
        return
    for claim in claims.get("P1696", []) or []:
        try:
            val = claim["mainsnak"]["datavalue"]["value"]
        except (KeyError, TypeError):
            continue
        if isinstance(val, dict) and "id" in val:
            yield val["id"]


def load_p1696_inverses() -> int:
    inserted = 0
    with connect() as conn:
        with conn.cursor(name="iter_props") as read_cur:
            read_cur.itersize = 1000
            read_cur.execute("SELECT pid, claims FROM wd_properties")
            with conn.cursor() as write_cur:
                for row in read_cur:
                    a = row["pid"]
                    for b in _iter_p1696(row["claims"]):
                        write_cur.execute(
                            "INSERT INTO inverse_properties (pid_a, pid_b, source) "
                            "VALUES (%s, %s, 'P1696'), (%s, %s, 'P1696') "
                            "ON CONFLICT DO NOTHING",
                            (a, b, b, a),
                        )
                        inserted += write_cur.rowcount
        conn.commit()
    return inserted
