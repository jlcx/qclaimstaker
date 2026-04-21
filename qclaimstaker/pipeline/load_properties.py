"""Load wd_properties from a wbgetentities-style JSON dump of all properties.

Expected shape:
  { "P6": { "id": "P6", "labels": {...}, "claims": {...}, "datatype": "...", ... },
    "P17": { ... }, ... }

`labels` and `claims` are stored as-is into jsonb columns. Other top-level
fields (type, datatype, pageid, modified, etc.) are ignored for now.
"""

from __future__ import annotations

import json
from pathlib import Path

import psycopg

from ..db import connect


def load_properties_json(path: str | Path, dump_version: str, truncate: bool = True) -> int:
    """Populate wd_properties from the given JSON file. Returns rows written."""
    p = Path(path)
    with p.open("rb") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(
            f"expected top-level JSON object mapping pid -> entity, got {type(data).__name__}"
        )

    rows = [
        (
            pid,
            psycopg.types.json.Jsonb(entity.get("labels") or {}),
            psycopg.types.json.Jsonb(entity.get("claims") or {}),
            dump_version,
        )
        for pid, entity in data.items()
        if isinstance(pid, str) and pid.startswith("P")
    ]

    with connect() as conn, conn.cursor() as cur:
        if truncate:
            cur.execute("TRUNCATE wd_properties")
        cur.executemany(
            """
            INSERT INTO wd_properties (pid, labels, claims, dump_version)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (pid) DO UPDATE
              SET labels = EXCLUDED.labels,
                  claims = EXCLUDED.claims,
                  dump_version = EXCLUDED.dump_version
            """,
            rows,
        )
        conn.commit()
    return len(rows)
