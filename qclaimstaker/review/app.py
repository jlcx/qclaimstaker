"""Thin review UI. Lists review_queue pairs for the active dump_version,
per-pair page shows src/dst labels + top-k ranked candidates, reviewer can
accept one, reject with reason, or propose a different property."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..config import settings
from ..db import connect

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

app = FastAPI(title="qclaimstaker review")


def _label(cur, qid: str) -> str:
    cur.execute(
        "SELECT label AS l FROM wd_labels WHERE qid = %s AND lang = %s LIMIT 1",
        (qid, settings.review_lang),
    )
    row = cur.fetchone()
    return (row and row["l"]) or qid


@app.get("/", response_class=HTMLResponse)
def queue(request: Request, limit: int = 50, offset: int = 0):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT src, dst, MAX(score) AS top_score, MAX(wp_count) AS wp_count
            FROM proposals
            WHERE dump_version = %s AND tier = 'review_queue' AND rank = 1
            GROUP BY src, dst
            ORDER BY top_score DESC
            LIMIT %s OFFSET %s
            """,
            (settings.dump_version, limit, offset),
        )
        rows = cur.fetchall()
        for r in rows:
            r["src_label"] = _label(cur, r["src"])
            r["dst_label"] = _label(cur, r["dst"])
    return TEMPLATES.TemplateResponse(
        request,
        "queue.html",
        {
            "rows": rows,
            "dump_version": settings.dump_version,
            "offset": offset,
            "limit": limit,
        },
    )


@app.get("/pair/{src}/{dst}", response_class=HTMLResponse)
def pair(request: Request, src: str, dst: str):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.pid, p.score, p.margin, p.direction, p.rank, pc.label
            FROM proposals p
            LEFT JOIN property_constraints pc ON pc.pid = p.pid
            WHERE p.dump_version = %s AND p.src = %s AND p.dst = %s
            ORDER BY p.rank
            """,
            (settings.dump_version, src, dst),
        )
        candidates = cur.fetchall()
        src_label = _label(cur, src)
        dst_label = _label(cur, dst)
    return TEMPLATES.TemplateResponse(
        request,
        "pair.html",
        {
            "src": src, "dst": dst,
            "src_label": src_label, "dst_label": dst_label,
            "candidates": candidates,
            "reviewer": settings.reviewer_name,
        },
    )


@app.post("/pair/{src}/{dst}/decide")
def decide(
    src: str, dst: str,
    decision: str = Form(...),
    pid: str = Form(""),
    other_pid: str = Form(""),
    reject_reason: str = Form(""),
    note: str = Form(""),
    reviewer: str = Form(""),
):
    if decision not in ("accept", "reject", "other"):
        return RedirectResponse(f"/pair/{src}/{dst}", status_code=303)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reviews
              (src, dst, pid, decision, other_pid, reject_reason, reviewer, note)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                src, dst, pid or (other_pid or ""),
                decision,
                other_pid or None,
                reject_reason or None,
                reviewer or settings.reviewer_name,
                note or None,
            ),
        )
        conn.commit()
    return RedirectResponse("/", status_code=303)
