from __future__ import annotations

from pathlib import Path

from ..db import connect

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"

MIGRATIONS = [
    "001_expected_source_tables.sql",
    "002_derived_tables.sql",
    "003_seed_inverses.sql",
    "004_seed_blocklists.sql",
    "010_subclass_closure.sql",
    "011_transitive_paths.sql",
    "012_candidate_refresh.sql",
]


def apply_all() -> None:
    with connect() as conn:
        for name in MIGRATIONS:
            sql = (SQL_DIR / name).read_text()
            with conn.cursor() as cur:
                cur.execute(sql)
        conn.commit()


def record_dump(dump_version: str, notes: str | None = None) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dump_versions (dump_version, notes) VALUES (%s, %s) "
            "ON CONFLICT (dump_version) DO UPDATE SET notes = EXCLUDED.notes",
            (dump_version, notes),
        )
        conn.commit()
