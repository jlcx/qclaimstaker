"""QuickStatements v2 CSV emitter.

QS v2 CSV shape (one row per statement):
  qid,P<pid>,<value>,S854,"<ref url>",...
We emit only item-valued statements, so values are Q-ids.

Reference: per SPEC §7, the auto-queue batch carries
  "inferred from Wikipedia link graph, dump version X".
For CSV we put this in S887 (based on heuristic) as a free-form string;
imagers and reviewers should swap it for a concrete source if desired.
"""

from __future__ import annotations

import csv
from pathlib import Path

from ..config import settings
from ..db import connect


def emit_auto_queue(dump_version: str, out_dir: str | None = None) -> Path:
    out_base = Path(out_dir or settings.qs_output_dir)
    out_base.mkdir(parents=True, exist_ok=True)
    path = out_base / f"auto_queue_{dump_version}.csv"
    ref_text = settings.qs_reference_template.format(dump_version=dump_version)

    with connect() as conn, conn.cursor() as cur, path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["qid", "property", "value", "S887"])
        cur.execute(
            """
            SELECT src, dst, pid, direction
            FROM proposals
            WHERE dump_version = %s AND tier = 'auto_queue' AND rank = 1
            ORDER BY score DESC
            """,
            (dump_version,),
        )
        for row in cur.fetchall():
            if row["direction"] == "inverse":
                subject, obj = row["dst"], row["src"]
            else:
                subject, obj = row["src"], row["dst"]
            w.writerow([subject, row["pid"], obj, f'"{ref_text}"'])
    return path
