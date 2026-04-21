from __future__ import annotations

import typer
import uvicorn

from .config import settings
from .pipeline import candidates, constraints, emitter, inverse, schema, tiering
from .pipeline import load_properties as load_properties_mod
from .pipeline import type_resolution

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command("init-db")
def init_db():
    """Apply all SQL migrations + seeds. Idempotent."""
    schema.apply_all()
    typer.echo("schema applied")


@app.command("record-dump")
def record_dump(dump_version: str, notes: str = ""):
    schema.record_dump(dump_version, notes or None)
    typer.echo(f"recorded {dump_version}")


@app.command("rebuild-types")
def rebuild_types():
    """Populate direct_types from wd_links (prop='P31'), then refresh closures."""
    n = type_resolution.refresh_direct_types()
    typer.echo(f"direct_types rows: {n}")
    sc, tc = type_resolution.refresh_closures()
    typer.echo(f"subclass_closure rows: {sc}; type_closure rows: {tc}")


@app.command("load-properties")
def load_properties(
    path: str = "wikidata_properties_full.json",
    dump_version: str = "",
    no_truncate: bool = False,
):
    """Ingest all property entities from a wbgetentities-style JSON dump."""
    dv = dump_version or settings.dump_version
    n = load_properties_mod.load_properties_json(path, dv, truncate=not no_truncate)
    typer.echo(f"wd_properties rows: {n}")


@app.command("load-inverses")
def load_inverses():
    n = inverse.load_p1696_inverses()
    typer.echo(f"P1696 inverse rows added: {n}")


@app.command("load-constraints")
def load_constraints():
    n = constraints.refresh_property_constraints()
    typer.echo(f"property_constraints rows: {n}")


@app.command("refresh-transitive")
def refresh_transitive():
    n = candidates.refresh_transitive_paths()
    typer.echo(f"transitive_paths rows: {n}")


@app.command("refresh-candidates")
def refresh_candidates(dump_version: str = ""):
    dv = dump_version or settings.dump_version
    n_pairs = candidates.refresh_candidate_pairs(dv)
    typer.echo(f"candidate_pairs: {n_pairs}")
    n_props = candidates.refresh_candidate_properties(dv)
    typer.echo(f"candidate_properties: {n_props}")


@app.command("refresh-prior")
def refresh_prior():
    n = candidates.refresh_type_pair_prior()
    typer.echo(f"type_pair_prior rows: {n}")


@app.command("rank")
def rank(dump_version: str = ""):
    dv = dump_version or settings.dump_version
    counts = tiering.rank_and_tier(dv)
    typer.echo(f"tiers: {counts}")


@app.command("emit-qs")
def emit_qs(dump_version: str = "", out_dir: str = ""):
    dv = dump_version or settings.dump_version
    path = emitter.emit_auto_queue(dv, out_dir or None)
    typer.echo(f"wrote {path}")


@app.command("run-all")
def run_all(dump_version: str):
    """Convenience: end-to-end. Assumes wp_links, wd_links, and wd_properties
    are already populated (and optionally wd_labels for nicer review UI)."""
    schema.apply_all()
    schema.record_dump(dump_version)
    type_resolution.refresh_direct_types()
    type_resolution.refresh_closures()
    inverse.load_p1696_inverses()
    constraints.refresh_property_constraints()
    candidates.refresh_transitive_paths()
    candidates.refresh_candidate_pairs(dump_version)
    candidates.refresh_candidate_properties(dump_version)
    candidates.refresh_type_pair_prior()
    counts = tiering.rank_and_tier(dump_version)
    typer.echo(f"tiers: {counts}")
    path = emitter.emit_auto_queue(dump_version)
    typer.echo(f"auto-queue CSV: {path}")


@app.command("serve")
def serve(host: str = "", port: int = 0):
    uvicorn.run(
        "qclaimstaker.review.app:app",
        host=host or settings.review_bind,
        port=port or settings.review_port,
        reload=False,
    )


if __name__ == "__main__":
    app()
