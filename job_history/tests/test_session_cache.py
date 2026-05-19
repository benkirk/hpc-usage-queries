"""Tests for the engine cache in job_history.database.session.

Covers the memoization behavior added so long-lived consumers (Flask webapps,
daemons) reuse a single Engine per (backend-target, pool_kwargs).
"""

import pytest

from job_history.database import clear_engine_cache, get_engine, get_session


@pytest.fixture(autouse=True)
def _isolate_cache():
    """Each test starts and ends with a clean engine cache."""
    clear_engine_cache()
    yield
    clear_engine_cache()


@pytest.fixture
def per_machine_dbs(tmp_path, monkeypatch):
    """Point both machines at sandboxed SQLite paths via QHIST_*_DB env vars."""
    derecho_db = tmp_path / "derecho.db"
    casper_db = tmp_path / "casper.db"
    monkeypatch.setenv("QHIST_DERECHO_DB", str(derecho_db))
    monkeypatch.setenv("QHIST_CASPER_DB", str(casper_db))
    # Force SQLite backend regardless of ambient env.
    monkeypatch.setattr(
        "job_history.database.session.JobHistoryConfig.DB_BACKEND",
        "sqlite",
        raising=False,
    )
    return derecho_db, casper_db


def test_get_engine_returns_same_instance(per_machine_dbs):
    e1 = get_engine("derecho")
    e2 = get_engine("derecho")
    assert e1 is e2


def test_different_machines_get_different_engines(per_machine_dbs):
    e_d = get_engine("derecho")
    e_c = get_engine("casper")
    assert e_d is not e_c


def test_clear_engine_cache_yields_fresh_engine(per_machine_dbs):
    e1 = get_engine("derecho")
    clear_engine_cache()
    e2 = get_engine("derecho")
    assert e1 is not e2


def test_pool_kwargs_keys_separately(per_machine_dbs):
    """Different pool_kwargs produce distinct cache entries (different Engines)."""
    e_default = get_engine("derecho")
    e_tuned = get_engine("derecho", pool_kwargs={"pool_pre_ping": True})
    assert e_default is not e_tuned
    # Repeated call with same pool_kwargs is still cached.
    e_tuned2 = get_engine("derecho", pool_kwargs={"pool_pre_ping": True})
    assert e_tuned is e_tuned2


def test_get_session_uses_cached_engine(per_machine_dbs):
    s1 = get_session("derecho")
    s2 = get_session("derecho")
    try:
        assert s1.get_bind() is s2.get_bind()
    finally:
        s1.close()
        s2.close()


def test_unknown_machine_raises(per_machine_dbs):
    with pytest.raises(ValueError):
        get_engine("gust")
