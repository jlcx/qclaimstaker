from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from .config import settings


@contextmanager
def connect(autocommit: bool = False) -> Iterator[psycopg.Connection]:
    with psycopg.connect(settings.dsn, autocommit=autocommit, row_factory=dict_row) as conn:
        yield conn


@contextmanager
def cursor(autocommit: bool = False) -> Iterator[psycopg.Cursor]:
    with connect(autocommit=autocommit) as conn:
        with conn.cursor() as cur:
            yield cur
            if not autocommit:
                conn.commit()
