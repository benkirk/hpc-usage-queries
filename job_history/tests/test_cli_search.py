"""Tests for the ``jobhist search`` subcommand.

Covers:
- builder (pure function, synthetic rows)
- SearchCommand (real in-memory DB + query layer)
- column resolution precedence (--display > --verbose > defaults)
- Click integration through the new entry point
"""

import io
import json
from datetime import date, datetime, timedelta, timezone

import pytest
from rich.console import Console

from job_history.database import Job, JobCharge
from job_history.cli.core import Context
from job_history.cli.search import (
    SearchCommand,
    COLUMNS,
    DEFAULT_COLUMNS,
    VERBOSE_COLUMNS,
)
from job_history.cli.search import builders
from job_history.cli.search.commands import _resolve_columns


# ---------------------------------------------------------------------------
# Builder / column-resolution tests — pure functions, no DB
# ---------------------------------------------------------------------------

def _ctx(start=date(2025, 1, 14), end=date(2025, 1, 17), machine="derecho"):
    ctx = Context()
    ctx.start_date = start
    ctx.end_date = end
    ctx.machine = machine
    return ctx


class TestBuildSearch:
    def test_envelope_kind_and_metadata(self):
        env = builders.build_search(
            [], ctx=_ctx(),
            requested_cols=DEFAULT_COLUMNS,
            filters={"user": "alice", "account": None, "queue": None, "status": None},
        )
        assert env["kind"] == "search"
        assert env["machine"] == "derecho"
        assert env["start"] == date(2025, 1, 14)
        assert env["end"] == date(2025, 1, 17)
        assert env["filters"]["user"] == "alice"
        assert env["rows"] == []

    def test_envelope_column_specs_match_registry(self):
        env = builders.build_search(
            [], ctx=_ctx(),
            requested_cols=("job_id", "user", "cpu_hours"),
        )
        keys = [c["key"] for c in env["columns"]]
        assert keys == ["job_id", "user", "cpu_hours"]
        headers = [c["header"] for c in env["columns"]]
        assert headers == [COLUMNS["job_id"]["header"],
                           COLUMNS["user"]["header"],
                           COLUMNS["cpu_hours"]["header"]]


class TestResolveColumns:
    def test_defaults_when_unset(self):
        assert _resolve_columns(display=None, verbose=False) == DEFAULT_COLUMNS

    def test_verbose_returns_all(self):
        assert _resolve_columns(display=None, verbose=True) == VERBOSE_COLUMNS

    def test_display_overrides_verbose(self):
        cols = _resolve_columns(display="job_id,user", verbose=True)
        assert cols == ("job_id", "user")

    def test_display_strips_whitespace(self):
        cols = _resolve_columns(display=" job_id , user , cpu_hours ", verbose=False)
        assert cols == ("job_id", "user", "cpu_hours")

    def test_display_unknown_column_raises(self):
        with pytest.raises(ValueError, match="Unknown column"):
            _resolve_columns(display="job_id,bogus", verbose=False)


# ---------------------------------------------------------------------------
# SearchCommand — real DB, full query + builder + exporter chain
# ---------------------------------------------------------------------------

@pytest.fixture
def search_db(in_memory_session):
    """Two jobs (one cpu, one gpu) for alice with charges populated."""
    base = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    jobs = [
        Job(
            job_id="200.desched1", short_id=200, user="alice",
            account="NCAR0001", queue="main", status="F",
            submit=base, start=base, end=base + timedelta(hours=1),
            elapsed=3600, numcpus=128, numgpus=0, numnodes=1,
        ),
        Job(
            job_id="201.desched1", short_id=201, user="bob",
            account="NCAR0002", queue="gpudev", status="F",
            submit=base, start=base, end=base + timedelta(hours=2),
            elapsed=7200, numcpus=64, numgpus=4, numnodes=1,
        ),
    ]
    for j in jobs:
        in_memory_session.add(j)
    in_memory_session.flush()
    in_memory_session.add_all([
        JobCharge(job_id=jobs[0].id, cpu_hours=128.0, gpu_hours=0.0,
                  memory_hours=10.0, qos_factor=1.0, charge_version=1),
        JobCharge(job_id=jobs[1].id, cpu_hours=64.0, gpu_hours=16.0,
                  memory_hours=20.0, qos_factor=1.0, charge_version=1),
    ])
    in_memory_session.commit()
    return jobs


@pytest.fixture
def search_ctx(in_memory_session, search_db):
    ctx = _ctx()
    ctx.session = in_memory_session
    return ctx


def _capture_console(ctx) -> io.StringIO:
    buf = io.StringIO()
    ctx.console = Console(file=buf, force_terminal=False, width=200)
    return buf


class TestSearchCommand:
    def test_rich_output_default_columns(self, search_ctx):
        buf = _capture_console(search_ctx)
        code = SearchCommand(search_ctx).execute()
        assert code == 0
        out = buf.getvalue()
        assert "alice" in out
        assert "bob" in out
        assert "Job ID" in out  # header

    def test_json_envelope(self, search_ctx, capsys):
        search_ctx.output_format = "json"
        code = SearchCommand(search_ctx).execute()
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["kind"] == "search"
        assert parsed["machine"] == "derecho"
        assert len(parsed["rows"]) == 2
        # filters block exists even when empty
        assert "filters" in parsed
        # row keys match the (default) columns
        for row in parsed["rows"]:
            assert set(row.keys()) == set(DEFAULT_COLUMNS)

    def test_user_filter_narrows_results(self, search_ctx, capsys):
        search_ctx.output_format = "json"
        code = SearchCommand(search_ctx).execute(user="alice")
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed["rows"]) == 1
        assert parsed["rows"][0]["user"] == "alice"
        assert parsed["filters"]["user"] == "alice"

    def test_verbose_widens_columns(self, search_ctx, capsys):
        search_ctx.output_format = "json"
        code = SearchCommand(search_ctx).execute(verbose=True)
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        for row in parsed["rows"]:
            assert set(row.keys()) == set(VERBOSE_COLUMNS)

    def test_display_overrides_columns(self, search_ctx, capsys):
        search_ctx.output_format = "json"
        code = SearchCommand(search_ctx).execute(display="job_id,user,cpu_hours")
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        for row in parsed["rows"]:
            assert set(row.keys()) == {"job_id", "user", "cpu_hours"}

    def test_invalid_display_returns_error(self, search_ctx, capsys):
        search_ctx.output_format = "json"
        code = SearchCommand(search_ctx).execute(display="job_id,bogus")
        assert code != 0  # EXIT_ERROR
        # Nothing emitted to stdout
        assert capsys.readouterr().out == ""

    def test_limit_flag_truncates_and_appears_in_filters(self, search_ctx, capsys):
        search_ctx.output_format = "json"
        code = SearchCommand(search_ctx).execute(limit=1)
        assert code == 0
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed["rows"]) == 1
        assert parsed["filters"]["limit"] == 1


# ---------------------------------------------------------------------------
# CliRunner — Click integration through the new entry point
# ---------------------------------------------------------------------------

class TestJobhistSearchCli:
    def test_search_appears_in_top_level_help(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "search" in result.output

    def test_search_help(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["search", "--help"])
        assert result.exit_code == 0
        for opt in ("--start-date", "--end-date", "-m, --machine",
                    "--user", "--project", "--queue",
                    "-v, --verbose", "--display"):
            assert opt in result.output

    def test_invalid_date_format(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(
            cli, ["search", "--start-date", "not-a-date", "-m", "derecho"]
        )
        assert result.exit_code != 0
        assert "YYYY-MM-DD" in result.output

    def test_invalid_machine(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["search", "-m", "fugaku"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()

    def test_limit_rejects_non_positive(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["search", "--limit", "0"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output
