"""Tests for the SAM-aligned history subcommands.

Covers:
- builders (pure functions, synthetic rows)
- command classes (real in-memory DB, real query layer)
- Click integration via CliRunner (end-to-end through the new jobhist entry)
"""

import io
import json
from datetime import date, timedelta

import pytest
from rich.console import Console

from job_history.database import DailySummary
from job_history.cli.core import Context
from job_history.cli.history import (
    JobsPerUserCommand,
    JobsPerProjectCommand,
    UniqueProjectsCommand,
    UniqueUsersCommand,
    DailySummaryCommand,
)
from job_history.cli.history import builders


# ---------------------------------------------------------------------------
# Builder tests — pure functions, no DB
# ---------------------------------------------------------------------------

def _ctx(start=date(2026, 1, 1), end=date(2026, 1, 31), group_by="day", machine="derecho"):
    ctx = Context()
    ctx.start_date = start
    ctx.end_date = end
    ctx.group_by = group_by
    ctx.machine = machine
    return ctx


class TestBuildJobsPerEntity:
    def test_user_primary_non_verbose_collapses_account(self):
        rows = [
            {"period": "2026-01-15", "user": "alice", "account": "NCAR0001", "job_count": 5},
            {"period": "2026-01-15", "user": "alice", "account": "NCAR0002", "job_count": 3},
            {"period": "2026-01-15", "user": "bob",   "account": "NCAR0001", "job_count": 2},
        ]
        env = builders.build_jobs_per_entity(rows, ctx=_ctx(), primary_entity="user", verbose=False)
        assert env["kind"] == "jobs_per_user"
        assert env["primary_entity"] == "user"
        assert env["verbose"] is False
        # No account column in non-verbose
        headers = [c["header"] for c in env["columns"]]
        assert "Account" not in headers
        # alice's two rows collapse to one (5+3=8)
        alice_rows = [r for r in env["rows"] if r["user"] == "alice"]
        assert len(alice_rows) == 1
        assert alice_rows[0]["job_count"] == 8

    def test_user_primary_verbose_keeps_account(self):
        rows = [
            {"period": "2026-01-15", "user": "alice", "account": "NCAR0001", "job_count": 5},
            {"period": "2026-01-15", "user": "alice", "account": "NCAR0002", "job_count": 3},
        ]
        env = builders.build_jobs_per_entity(rows, ctx=_ctx(), primary_entity="user", verbose=True)
        headers = [c["header"] for c in env["columns"]]
        assert "Account" in headers
        # No collapsing
        assert len(env["rows"]) == 2

    def test_account_primary_kind(self):
        rows = [{"period": "2026-01-15", "user": "alice", "account": "NCAR0001", "job_count": 5}]
        env = builders.build_jobs_per_entity(rows, ctx=_ctx(), primary_entity="account", verbose=False)
        assert env["kind"] == "jobs_per_account"
        assert env["primary_entity"] == "account"

    def test_envelope_carries_context_metadata(self):
        ctx = _ctx(machine="casper", group_by="month")
        env = builders.build_jobs_per_entity([], ctx=ctx, primary_entity="user", verbose=False)
        assert env["machine"] == "casper"
        assert env["group_by"] == "month"
        assert env["start"] == date(2026, 1, 1)
        assert env["end"] == date(2026, 1, 31)


class TestBuildUnique:
    def test_unique_projects_envelope(self):
        rows = [{"period": "2026-01", "project_count": 5}]
        env = builders.build_unique_projects(rows, ctx=_ctx())
        assert env["kind"] == "unique_projects"
        assert env["rows"] == rows

    def test_unique_users_envelope(self):
        rows = [{"period": "2026-01", "user_count": 12}]
        env = builders.build_unique_users(rows, ctx=_ctx())
        assert env["kind"] == "unique_users"
        assert env["rows"] == rows


class TestBuildDailySummary:
    def test_envelope_shape(self):
        rows = [
            {"date": "2026-01-15", "user": "alice", "account": "NCAR0001", "queue": "main",
             "job_count": 5, "cpu_hours": 80.0, "gpu_hours": 0.0, "memory_hours": 50.0},
        ]
        env = builders.build_daily_summary(rows, ctx=_ctx())
        assert env["kind"] == "daily_summary"
        headers = [c["header"] for c in env["columns"]]
        assert headers == ["Date", "User", "Account", "Queue", "Jobs", "CPU-h", "GPU-h", "Mem-h"]
        assert env["rows"] == rows


# ---------------------------------------------------------------------------
# Command tests — exercise the full query + builder + exporter chain
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_daily_summaries(in_memory_session):
    """Three daily-summary rows: alice (NCAR0001/main) ×2, bob (NCAR0002/gpudev) ×1."""
    base = date(2025, 1, 15)
    rows = [
        DailySummary(date=base, user="alice", account="NCAR0001", queue="main",
                     job_count=5, cpu_hours=80.0, gpu_hours=0.0, memory_hours=50.0),
        DailySummary(date=base + timedelta(days=1), user="alice", account="NCAR0001", queue="main",
                     job_count=3, cpu_hours=50.0, gpu_hours=0.0, memory_hours=30.0),
        DailySummary(date=base, user="bob", account="NCAR0002", queue="gpudev",
                     job_count=2, cpu_hours=20.0, gpu_hours=10.0, memory_hours=15.0),
    ]
    for r in rows:
        in_memory_session.add(r)
    in_memory_session.commit()
    return rows


@pytest.fixture
def daily_summary_ctx(in_memory_session, sample_daily_summaries):
    """Context wired with a real in-memory session + sample daily summaries."""
    ctx = _ctx(start=date(2025, 1, 15), end=date(2025, 1, 16))
    ctx.session = in_memory_session
    return ctx


def _capture_console(ctx) -> io.StringIO:
    """Redirect ``ctx.console`` output to an in-memory buffer."""
    buf = io.StringIO()
    ctx.console = Console(file=buf, force_terminal=False, width=160)
    return buf


class TestDailySummaryCommand:
    def test_rich_output(self, daily_summary_ctx):
        buf = _capture_console(daily_summary_ctx)
        code = DailySummaryCommand(daily_summary_ctx).execute()
        assert code == 0
        out = buf.getvalue()
        assert "alice" in out
        assert "bob" in out
        assert "Date" in out  # header
        assert "Mem-h" in out

    def test_json_output(self, daily_summary_ctx, capsys):
        daily_summary_ctx.output_format = "json"
        code = DailySummaryCommand(daily_summary_ctx).execute()
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["kind"] == "daily_summary"
        assert parsed["machine"] == "derecho"
        assert parsed["start"] == "2025-01-15"
        assert len(parsed["rows"]) == 3
        # ensure floats round-trip
        for r in parsed["rows"]:
            assert isinstance(r["cpu_hours"], float)


class TestUniqueProjectsAndUsersCommands:
    def test_unique_projects_envelope_shape(self, in_memory_session, capsys):
        # unique_projects_by_period reads from the jobs table; without sample
        # jobs the result is empty. We assert the envelope shape only;
        # behaviour over real data is covered by tests/test_queries.py.
        ctx = _ctx(start=date(2025, 1, 15), end=date(2025, 1, 16))
        ctx.session = in_memory_session
        ctx.output_format = "json"
        code = UniqueProjectsCommand(ctx).execute()
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["kind"] == "unique_projects"
        assert parsed["group_by"] == "day"
        assert [c["header"] for c in parsed["columns"]] == ["Period", "Unique Projects"]

    def test_unique_users_envelope_shape(self, in_memory_session, capsys):
        ctx = _ctx(start=date(2025, 1, 15), end=date(2025, 1, 16))
        ctx.session = in_memory_session
        ctx.output_format = "json"
        code = UniqueUsersCommand(ctx).execute()
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["kind"] == "unique_users"
        assert [c["header"] for c in parsed["columns"]] == ["Period", "Unique Users"]

    def test_group_by_override(self, in_memory_session, capsys):
        ctx = _ctx(start=date(2025, 1, 1), end=date(2025, 1, 31), group_by="day")
        ctx.session = in_memory_session
        ctx.output_format = "json"
        code = UniqueProjectsCommand(ctx).execute(group_by="month")
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["group_by"] == "month"
        # And ctx.group_by must be restored to the original value.
        assert ctx.group_by == "day"


class TestJobsPerEntityCommand:
    def test_handles_empty_window(self, in_memory_session, capsys):
        ctx = _ctx(start=date(2030, 1, 1), end=date(2030, 1, 2))
        ctx.session = in_memory_session
        ctx.output_format = "json"
        code = JobsPerUserCommand(ctx).execute(verbose=False)
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["kind"] == "jobs_per_user"
        assert parsed["rows"] == []


# ---------------------------------------------------------------------------
# CliRunner — Click integration through the new entry point
# ---------------------------------------------------------------------------

class TestJobhistCli:
    def test_top_level_help(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "--format" in result.output
        assert "history" in result.output

    def test_history_help(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["history", "--help"])
        assert result.exit_code == 0
        for sub in ("jobs-per-user", "jobs-per-project", "unique-projects",
                    "unique-users", "daily-summary"):
            assert sub in result.output

    def test_format_option_validation(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["--format", "xml", "history", "--help"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()
