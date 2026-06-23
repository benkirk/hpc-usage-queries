"""Tests for the optional ``database=`` parameter (multi-CNPG-database support).

A single process must be able to query more than one PostgreSQL database on the
same CNPG cluster — e.g. ``campaign`` (Campaign_Store) and ``desc1`` (Destor) —
without any global ``FS_SCAN_PG_DB`` swap. The selector threads from
``FsScanQueries(database=...)`` down through the query helpers into
``get_engine(database=...)``, where it becomes part of the engine URL and the
engine cache key so engines for different databases never alias.

These tests are connection-free: no live postgres is contacted. Engine
*construction* still imports the dbapi (SQLAlchemy resolves the dialect's
driver in ``create_engine``, not at connect time), so the one test that builds
a postgres engine ``importorskip("psycopg2")`` — it runs under the postgres
backend CI job and is skipped on the psycopg2-less sqlite runner. The helper
forwarding tests stub ``get_session``/discovery and build no engine, so they
run everywhere.
"""

from unittest.mock import MagicMock

import pytest

import fs_scans.core.config as cfg
from fs_scans.core.database import clear_engine_cache, get_engine


def _set_pg_config(monkeypatch):
    """Point FsScanConfig at a fake postgres so validate_postgres() passes."""
    monkeypatch.setattr(cfg.FsScanConfig, "DB_BACKEND", "postgres")
    monkeypatch.setattr(cfg.FsScanConfig, "PG_HOST", "fake-host")
    monkeypatch.setattr(cfg.FsScanConfig, "PG_PORT", 5432)
    monkeypatch.setattr(cfg.FsScanConfig, "PG_USER", "tester")
    monkeypatch.setattr(cfg.FsScanConfig, "PG_PASSWORD", "secret")
    monkeypatch.setattr(cfg.FsScanConfig, "PG_REQUIRE_SSL", False)
    monkeypatch.setattr(cfg.FsScanConfig, "PG_DB_NAME", "campaign")


def test_get_engine_database_selects_url_and_cache_key(monkeypatch):
    """database= drives the connection URL's db name and the engine cache key."""
    # create_engine imports the dialect's dbapi eagerly; skip where it's absent
    # (the sqlite-backend CI job). The postgres-backend job exercises this.
    pytest.importorskip("psycopg2")
    _set_pg_config(monkeypatch)
    clear_engine_cache()
    try:
        # Default → PG_DB_NAME.
        default_engine = get_engine("cisl")
        assert default_engine.url.database == "campaign"

        # Explicit databases produce distinct engines with the right db name...
        campaign_engine = get_engine("cisl", database="campaign")
        desc1_engine = get_engine("cisl", database="desc1")
        assert campaign_engine.url.database == "campaign"
        assert desc1_engine.url.database == "desc1"

        # ...and they are NOT the same cached object (cache key includes db).
        assert campaign_engine is not desc1_engine
        # Default and explicit-same-name share the cache entry (same key).
        assert default_engine is campaign_engine
        # Same (collection, database) is memoized.
        assert get_engine("cisl", database="desc1") is desc1_engine
    finally:
        clear_engine_cache()


def test_fsscanqueries_forwards_database_to_discovery(monkeypatch):
    """FsScanQueries stores self.database and forwards it when filesystems='all'."""
    import fs_scans.queries.facade as facade

    seen = {}

    def fake_get_all(database=None):
        seen["database"] = database
        return ["cisl", "mmm"]

    monkeypatch.setattr(facade, "get_all_filesystems", fake_get_all)

    q = facade.FsScanQueries(filesystems="all", database="desc1")
    assert q.database == "desc1"
    assert seen["database"] == "desc1"
    assert q.filesystems == ["cisl", "mmm"]

    # Explicit list does not hit discovery but still records the database.
    q2 = facade.FsScanQueries(filesystems=["cisl"], database="desc1")
    assert q2.database == "desc1"
    assert q2.filesystems == ["cisl"]


def test_resolve_usernames_forwards_database_to_get_session(monkeypatch):
    """The cross-database username resolver opens sessions on the chosen db."""
    import fs_scans.queries.query_engine as qe

    calls = []

    def fake_get_session(fs, database=None):
        calls.append((fs, database))
        return MagicMock()

    # No real lookup — force the str(uid) last-resort path by returning {}.
    monkeypatch.setattr(qe, "get_session", fake_get_session)
    monkeypatch.setattr(qe, "get_username_map", lambda *a, **k: {})

    result = qe.resolve_usernames_across_databases({4242}, ["cisl"], database="desc1")

    assert calls == [("cisl", "desc1")]
    assert result == {4242: "4242"}  # unresolved UID → str(uid)
