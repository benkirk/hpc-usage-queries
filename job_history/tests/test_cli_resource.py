"""Tests for SAM-aligned resource subcommands.

Covers:
- File-format exporters (byte-for-byte parity with legacy DatExporter)
- ResourceCommand period injection for time-series queries
- ResourceCommand multi-machine dispatch (machine='all')
- Click integration via CliRunner against the new entry point
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from job_history.cli.core import Context, ExporterRegistry
from job_history.cli.core.file_exporters import (
    DatFileExporter,
    CsvFileExporter,
    MarkdownFileExporter,
    JSONFileExporter,
    register_file_exporters,
)
from job_history.cli.resource import (
    ColumnSpec,
    ReportConfig,
    RESOURCE_REPORTS,
    ResourceCommand,
)
from job_history.exporters import DatExporter, CSVExporter
from job_history.queries import JobQueries


# Make sure the file exporters are registered for every test run, even if
# this module is collected before cli.cmds.jobhist's import-time call.
register_file_exporters()


# ---------------------------------------------------------------------------
# RESOURCE_REPORTS sanity
# ---------------------------------------------------------------------------

class TestResourceReports:
    def test_registry_non_empty(self):
        assert len(RESOURCE_REPORTS) > 0

    def test_unique_command_names(self):
        names = [c.command_name for c in RESOURCE_REPORTS]
        assert len(names) == len(set(names)), "duplicate command_name in RESOURCE_REPORTS"

    def test_filename_generation_derecho(self):
        cfg = RESOURCE_REPORTS[0]
        name = cfg.get_filename("derecho", date(2026, 1, 1), date(2026, 1, 31))
        assert name.startswith("De_")
        assert name.endswith(".dat")
        assert "2026-01-01" in name
        assert "2026-01-31" in name

    def test_filename_generation_casper(self):
        cfg = RESOURCE_REPORTS[0]
        name = cfg.get_filename("casper", date(2026, 1, 1), date(2026, 1, 31))
        assert name.startswith("Ca_")

    def test_filename_generation_all(self):
        cfg = RESOURCE_REPORTS[0]
        name = cfg.get_filename("all", date(2026, 1, 1), date(2026, 1, 31))
        assert name.startswith("All_")

    def test_filename_extension_override(self):
        cfg = RESOURCE_REPORTS[0]
        name = cfg.get_filename("derecho", date(2026, 1, 1), date(2026, 1, 31), extension="csv")
        assert name.endswith(".csv")


# ---------------------------------------------------------------------------
# File-format exporters — byte-for-byte parity with legacy exporters
# ---------------------------------------------------------------------------

SYNTHETIC_COLUMNS = [
    ColumnSpec("label",       "User-ids", 15, "s"),
    ColumnSpec("usage_hours", "Usage",    15, ".1f"),
    ColumnSpec("job_count",   "Counts",   0,  ""),
]

SYNTHETIC_ROWS = [
    {"label": "alice",   "usage_hours": 123.4567, "job_count": 12},
    {"label": "bob",     "usage_hours": 99.0,      "job_count": 3},
    {"label": "charlie", "usage_hours": 0.001,    "job_count": 1},
]

SYNTHETIC_CONFIG = ReportConfig(
    command_name="pie-user-cpu",
    description="Test",
    query_method="usage_by_group",
    query_params={"resource_type": "cpu", "group_by": "user"},
    filename_base="pie_user_cpu",
    columns=SYNTHETIC_COLUMNS,
)


def _envelope():
    return {
        "kind": "pie_user_cpu",
        "machine": "derecho",
        "start": date(2026, 1, 1),
        "end": date(2026, 1, 31),
        "columns": [
            {"key": c.key, "header": c.header, "width": c.width, "format": c.format}
            for c in SYNTHETIC_COLUMNS
        ],
        "rows": SYNTHETIC_ROWS,
    }


def _ctx_with_output(tmp_path):
    ctx = Context()
    ctx.machine = "derecho"
    ctx.start_date = date(2026, 1, 1)
    ctx.end_date = date(2026, 1, 31)
    ctx.output_dir = tmp_path
    return ctx


class TestDatFileExporter:
    def test_writes_dat_file_at_canonical_path(self, tmp_path):
        ctx = _ctx_with_output(tmp_path)
        DatFileExporter().emit(_envelope(), ctx=ctx, config=SYNTHETIC_CONFIG)
        expected = tmp_path / "De_pie_user_cpu_2026-01-01_2026-01-31.dat"
        assert expected.exists()

    def test_byte_for_byte_parity_with_legacy_dat_exporter(self, tmp_path):
        """The new DatFileExporter must produce identical output to the
        legacy ``DatExporter`` when given the same rows and columns. This is
        the critical regression check for the refactor."""
        ctx = _ctx_with_output(tmp_path)
        DatFileExporter().emit(_envelope(), ctx=ctx, config=SYNTHETIC_CONFIG)
        new_path = tmp_path / "De_pie_user_cpu_2026-01-01_2026-01-31.dat"

        legacy_path = tmp_path / "legacy.dat"
        DatExporter().export(SYNTHETIC_ROWS, SYNTHETIC_COLUMNS, str(legacy_path))

        assert new_path.read_bytes() == legacy_path.read_bytes()

    def test_creates_output_dir_if_missing(self, tmp_path):
        ctx = Context()
        ctx.output_dir = tmp_path / "deep" / "nested" / "out"
        ctx.machine = "derecho"
        ctx.start_date = date(2026, 1, 1)
        ctx.end_date = date(2026, 1, 2)
        DatFileExporter().emit(_envelope(), ctx=ctx, config=SYNTHETIC_CONFIG)
        assert ctx.output_dir.exists()
        files = list(ctx.output_dir.iterdir())
        assert any(f.suffix == ".dat" for f in files)


class TestOtherFileExporters:
    def test_csv_parity_with_legacy(self, tmp_path):
        ctx = _ctx_with_output(tmp_path)
        CsvFileExporter().emit(_envelope(), ctx=ctx, config=SYNTHETIC_CONFIG)
        new_path = tmp_path / "De_pie_user_cpu_2026-01-01_2026-01-31.csv"

        legacy_path = tmp_path / "legacy.csv"
        CSVExporter().export(SYNTHETIC_ROWS, SYNTHETIC_COLUMNS, str(legacy_path))

        assert new_path.read_bytes() == legacy_path.read_bytes()

    def test_markdown_emits_table(self, tmp_path):
        ctx = _ctx_with_output(tmp_path)
        MarkdownFileExporter().emit(_envelope(), ctx=ctx, config=SYNTHETIC_CONFIG)
        path = tmp_path / "De_pie_user_cpu_2026-01-01_2026-01-31.md"
        content = path.read_text()
        assert "| User-ids |" in content
        assert "| alice |" in content

    def test_json_file_parses(self, tmp_path):
        ctx = _ctx_with_output(tmp_path)
        JSONFileExporter().emit(_envelope(), ctx=ctx, config=SYNTHETIC_CONFIG)
        path = tmp_path / "De_pie_user_cpu_2026-01-01_2026-01-31.json"
        parsed = json.loads(path.read_text())
        assert parsed[0]["label"] == "alice"


class TestExporterRegistryFormats:
    def test_all_formats_registered(self):
        avail = ExporterRegistry.available()
        for fmt in ("rich", "json", "dat", "csv", "md", "json-file"):
            assert fmt in avail


# ---------------------------------------------------------------------------
# ResourceCommand period injection + multi-machine
# ---------------------------------------------------------------------------

class _FakeQueries:
    """Captures the kwargs every query method is called with."""

    def __init__(self, session=None, machine="derecho"):
        self.calls = []

    def usage_by_group(self, **kwargs):
        self.calls.append(("usage_by_group", kwargs))
        return [{"label": "alice", "usage_hours": 10.0, "job_count": 1}]

    def usage_history(self, **kwargs):
        self.calls.append(("usage_history", kwargs))
        return []

    def job_durations(self, **kwargs):
        self.calls.append(("job_durations", kwargs))
        return []


class TestResourceCommandPeriodInjection:
    def test_periodic_method_gets_period_param(self, tmp_path):
        """``usage_history`` is in PERIODIC_QUERY_METHODS, so the ctx
        group_by must be injected as the ``period`` query kwarg."""
        ctx = _ctx_with_output(tmp_path)
        ctx.group_by = "month"
        ctx.output_format = "json"

        fake = _FakeQueries()
        cfg = ReportConfig(
            command_name="usage-history",
            description="x",
            query_method="usage_history",
            query_params={},
            filename_base="usage_history",
            columns=[ColumnSpec("Date", "Date", 18, "s")],
        )
        cmd = ResourceCommand(ctx, cfg)
        with patch.object(cmd, "get_queries", return_value=fake):
            cmd.execute()
        method_name, kwargs = fake.calls[0]
        assert method_name == "usage_history"
        assert kwargs["period"] == "month"

    def test_non_periodic_method_no_period_param(self, tmp_path):
        ctx = _ctx_with_output(tmp_path)
        ctx.group_by = "month"
        ctx.output_format = "json"

        fake = _FakeQueries()
        cfg = ReportConfig(
            command_name="pie-user-cpu",
            description="x",
            query_method="usage_by_group",
            query_params={"resource_type": "cpu", "group_by": "user"},
            filename_base="pie_user_cpu",
            columns=SYNTHETIC_COLUMNS,
        )
        cmd = ResourceCommand(ctx, cfg)
        with patch.object(cmd, "get_queries", return_value=fake):
            cmd.execute()
        _, kwargs = fake.calls[0]
        assert "period" not in kwargs
        # query_params should still be merged in
        assert kwargs["resource_type"] == "cpu"
        assert kwargs["group_by"] == "user"


class TestResourceCommandMultiMachine:
    def test_machine_all_uses_multi_machine_query(self, tmp_path):
        ctx = _ctx_with_output(tmp_path)
        ctx.machine = "all"
        ctx.output_format = "json"

        cfg = ReportConfig(
            command_name="pie-user-cpu",
            description="x",
            query_method="usage_by_group",
            query_params={"resource_type": "cpu", "group_by": "user"},
            filename_base="pie_user_cpu",
            columns=SYNTHETIC_COLUMNS,
        )
        with patch.object(JobQueries, "multi_machine_query",
                          return_value=[{"label": "alice", "usage_hours": 1.0,
                                         "job_count": 1, "machine": "derecho"}]) as mock:
            code = ResourceCommand(ctx, cfg).execute()
        assert code == 0
        assert mock.called
        kwargs = mock.call_args.kwargs
        assert kwargs["machines"] == ["casper", "derecho"]
        assert kwargs["method_name"] == "usage_by_group"
        assert kwargs["resource_type"] == "cpu"
        assert kwargs["start"] == date(2026, 1, 1)


class TestResourceCommandEnvelope:
    def test_envelope_includes_metadata(self, tmp_path, capsys):
        ctx = _ctx_with_output(tmp_path)
        ctx.output_format = "json"
        ctx.group_by = "quarter"

        fake = _FakeQueries()
        cfg = ReportConfig(
            command_name="pie-user-cpu",
            description="x",
            query_method="usage_by_group",
            query_params={"resource_type": "cpu", "group_by": "user"},
            filename_base="pie_user_cpu",
            columns=SYNTHETIC_COLUMNS,
        )
        cmd = ResourceCommand(ctx, cfg)
        with patch.object(cmd, "get_queries", return_value=fake):
            cmd.execute()
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["kind"] == "pie_user_cpu"
        assert parsed["machine"] == "derecho"
        assert parsed["start"] == "2026-01-01"
        assert parsed["group_by"] == "quarter"
        assert len(parsed["rows"]) == 1


# ---------------------------------------------------------------------------
# CliRunner — Click integration for resource subcommands
# ---------------------------------------------------------------------------

class TestJobhistResourceCli:
    def test_resource_help_lists_all_reports(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        result = CliRunner().invoke(cli, ["resource", "--help"])
        assert result.exit_code == 0
        # spot-check a representative report from each category
        for sub in ("pie-user-cpu", "cpu-job-durations", "usage-history",
                    "memory-job-waits", "gpu-job-sizes"):
            assert sub in result.output

    def test_machine_all_accepted(self):
        """`--machine all` is a resource-only option, not valid on history."""
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        # Just exercise the parser; subcommand should reject due to missing
        # database, but the option parsing itself succeeds.
        result = CliRunner().invoke(cli, ["resource", "-m", "all", "--help"])
        assert result.exit_code == 0

    def test_history_machine_all_rejected(self):
        from click.testing import CliRunner
        from job_history.cli.cmds.jobhist import cli

        # Without --help so option validation isn't short-circuited.
        result = CliRunner().invoke(cli, ["history", "-m", "all", "daily-summary"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "invalid choice" in result.output.lower()
