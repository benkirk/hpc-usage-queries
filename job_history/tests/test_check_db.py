"""check_db (the DML-only sync's schema gate), --init-db, and PG URL quoting."""

import sqlite3

import pytest
from click.testing import CliRunner
from sqlalchemy.engine import make_url

from job_history.database import (
    JobHistoryConfig, SchemaNotReady, check_db, clear_engine_cache, init_db,
)
from job_history.sync.cli import sync


@pytest.fixture
def derecho_db(tmp_path, monkeypatch):
    path = tmp_path / "derecho.db"
    monkeypatch.setenv("QHIST_DERECHO_DB", str(path))
    monkeypatch.setattr(JobHistoryConfig, "DB_BACKEND", "sqlite")
    clear_engine_cache()
    yield path
    clear_engine_cache()


def _schema(path):
    with sqlite3.connect(path) as conn:
        return sorted(conn.execute("SELECT type, name, sql FROM sqlite_master").fetchall())


def _drop(path, ddl):
    clear_engine_cache()
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(ddl)


class TestCheckDb:
    def test_passes_after_init_db(self, derecho_db):
        init_db("derecho")
        assert check_db("derecho") is not None

    def test_missing_file_is_initialized(self, derecho_db):
        assert not derecho_db.exists()
        check_db("derecho")
        assert derecho_db.exists()
        check_db("derecho")

    def test_missing_table_raises(self, derecho_db):
        init_db("derecho")
        _drop(derecho_db, "DROP TABLE daily_summary")
        with pytest.raises(SchemaNotReady, match="daily_summary.*--init-db"):
            check_db("derecho")

    def test_missing_trigger_raises(self, derecho_db):
        init_db("derecho")
        _drop(derecho_db, "DROP TRIGGER trg_ensure_job_charge")
        with pytest.raises(SchemaNotReady, match="trg_ensure_job_charge"):
            check_db("derecho")

    def test_writes_no_schema(self, derecho_db):
        init_db("derecho")
        before = _schema(derecho_db)
        check_db("derecho")
        assert _schema(derecho_db) == before


class TestSyncCli:
    def test_init_db_alone_builds_and_exits(self, derecho_db):
        result = CliRunner().invoke(sync, ["-m", "derecho", "--init-db"])
        assert result.exit_code == 0, result.output
        assert "Initialized" in result.output
        assert ("trigger", "trg_ensure_job_charge") in {r[:2] for r in _schema(derecho_db)}

    def test_schema_behind_exits_2(self, derecho_db):
        init_db("derecho")
        _drop(derecho_db, "DROP TRIGGER trg_ensure_job_charge")
        result = CliRunner().invoke(sync, ["-m", "derecho", "--resummarize", "-d", "2026-01-29"])
        assert result.exit_code == 2
        assert "--init-db" in result.output

    def test_dry_run_writes_no_schema(self, derecho_db, tmp_path):
        init_db("derecho")
        before = _schema(derecho_db)
        logs = tmp_path / "logs"
        logs.mkdir()
        result = CliRunner().invoke(
            sync, ["-m", "derecho", "-l", str(logs), "-d", "2026-01-29", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert _schema(derecho_db) == before


def test_pg_url_quotes_password(monkeypatch):
    monkeypatch.setattr(JobHistoryConfig, "PG_PASSWORD", "p@ss:w/rd%#?")
    url = JobHistoryConfig.pg_url("derecho_jobs")
    assert make_url(url.render_as_string(hide_password=False)).password == "p@ss:w/rd%#?"
    assert "p@ss" not in url.render_as_string(hide_password=True)
