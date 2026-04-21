"""Parse P2302 property-constraint statements from wd_properties into
property_constraints.

Constraint qualifiers per Wikidata convention:
  - Subject-type constraint (Q21503250): classes in P2308, relation mode in P2309.
  - Value-type  constraint (Q21510865): classes in P2308, relation mode in P2309.
  - Conflicts-with (Q21502838): qualifier P2305 (other pid), optional P2306 (values).
  - Item-requires-statement (Q21503247): P2305 (pid), optional P2306 (values).
  - Value-requires-statement (Q21510864): P2305 (pid), optional P2306 (values).
  - One-of (Q21510859): P2305 values (allowed value list for the property value).
  - Exception-to-constraint (P2303): ignored-for-constraint qids.

(SPEC §5 claimed value-type uses P2309; empirically confirmed that P2308 is
correct for both subject and value. P2309 is always the relation mode.)
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from ..db import connect


CONSTRAINT_SUBJECT_TYPE = "Q21503250"
CONSTRAINT_VALUE_TYPE = "Q21510865"
CONSTRAINT_CONFLICTS_WITH = "Q21502838"
CONSTRAINT_ITEM_REQUIRES = "Q21503247"
CONSTRAINT_VALUE_REQUIRES = "Q21510864"
CONSTRAINT_ONE_OF = "Q21510859"


def _snak_qids(snaks: Iterable[dict[str, Any]] | None) -> list[str]:
    out: list[str] = []
    for s in snaks or []:
        try:
            v = s["datavalue"]["value"]
        except (KeyError, TypeError):
            continue
        if isinstance(v, dict) and "id" in v:
            out.append(v["id"])
    return out


def _constraint_type(claim: dict[str, Any]) -> str | None:
    try:
        v = claim["mainsnak"]["datavalue"]["value"]
    except (KeyError, TypeError):
        return None
    if isinstance(v, dict):
        return v.get("id")
    return None


def _qualifier_qids(claim: dict[str, Any], qpid: str) -> list[str]:
    return _snak_qids(claim.get("qualifiers", {}).get(qpid))


def _parse_property(claims: dict[str, Any]) -> dict[str, Any]:
    subject_types: list[str] = []
    value_types: list[str] = []
    conflicts_with: list[dict[str, Any]] = []
    requires: list[dict[str, Any]] = []
    value_requires: list[dict[str, Any]] = []
    one_of: list[str] = []
    exceptions: list[str] = []
    inverse_pid: str | None = None

    for claim in claims.get("P2302", []) or []:
        ctype = _constraint_type(claim)
        if ctype == CONSTRAINT_SUBJECT_TYPE:
            subject_types.extend(_qualifier_qids(claim, "P2308"))
            exceptions.extend(_qualifier_qids(claim, "P2303"))
        elif ctype == CONSTRAINT_VALUE_TYPE:
            value_types.extend(_qualifier_qids(claim, "P2308"))
            exceptions.extend(_qualifier_qids(claim, "P2303"))
        elif ctype == CONSTRAINT_CONFLICTS_WITH:
            for pid in _qualifier_qids(claim, "P2305"):
                conflicts_with.append(
                    {"pid": pid, "values": _qualifier_qids(claim, "P2306")}
                )
        elif ctype == CONSTRAINT_ITEM_REQUIRES:
            for pid in _qualifier_qids(claim, "P2305"):
                requires.append(
                    {"pid": pid, "values": _qualifier_qids(claim, "P2306")}
                )
        elif ctype == CONSTRAINT_VALUE_REQUIRES:
            for pid in _qualifier_qids(claim, "P2305"):
                value_requires.append(
                    {"pid": pid, "values": _qualifier_qids(claim, "P2306")}
                )
        elif ctype == CONSTRAINT_ONE_OF:
            one_of.extend(_qualifier_qids(claim, "P2305"))

    for claim in claims.get("P1696", []) or []:
        try:
            v = claim["mainsnak"]["datavalue"]["value"]
        except (KeyError, TypeError):
            continue
        if isinstance(v, dict) and "id" in v:
            inverse_pid = v["id"]
            break

    def j(xs):
        return json.dumps(xs) if xs else None

    return {
        "subject_types": j(sorted(set(subject_types))),
        "value_types": j(sorted(set(value_types))),
        "conflicts_with": json.dumps(conflicts_with) if conflicts_with else None,
        "one_of": j(sorted(set(one_of))),
        "requires": json.dumps(requires) if requires else None,
        "value_requires": json.dumps(value_requires) if value_requires else None,
        "exceptions": j(sorted(set(exceptions))),
        "inverse_pid": inverse_pid,
    }


def _label_en(labels: dict[str, Any] | None) -> str | None:
    if not labels:
        return None
    en = labels.get("en")
    if isinstance(en, dict):
        return en.get("value")
    return None


def refresh_property_constraints() -> int:
    """Rebuild property_constraints from wd_properties + usage counts."""
    n = 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE property_constraints")
        with conn.cursor(name="iter_props") as read_cur:
            read_cur.itersize = 500
            read_cur.execute("SELECT pid, labels, claims FROM wd_properties")
            with conn.cursor() as write_cur:
                for row in read_cur:
                    pid = row["pid"]
                    parsed = _parse_property(row["claims"] or {})
                    write_cur.execute(
                        """
                        INSERT INTO property_constraints
                          (pid, label, subject_types, value_types, conflicts_with,
                           one_of, requires, value_requires, exceptions,
                           usage_count, inverse_pid)
                        VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, 0, %s)
                        """,
                        (
                            pid,
                            _label_en(row["labels"]),
                            parsed["subject_types"],
                            parsed["value_types"],
                            parsed["conflicts_with"],
                            parsed["one_of"],
                            parsed["requires"],
                            parsed["value_requires"],
                            parsed["exceptions"],
                            parsed["inverse_pid"],
                        ),
                    )
                    n += 1
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE property_constraints pc
                SET usage_count = COALESCE(u.n, 0)
                FROM (SELECT prop AS pid, COUNT(*) AS n FROM wd_links GROUP BY prop) u
                WHERE u.pid = pc.pid
                """
            )
        conn.commit()
    return n
