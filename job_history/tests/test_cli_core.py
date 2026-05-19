"""Tests for job_history.cli.core — Context, ExporterRegistry, JSON encoder."""

import io
import json
from datetime import date, datetime
from decimal import Decimal

import pytest

from job_history.cli.core import (
    Context,
    Exporter,
    ExporterRegistry,
    JSONStdoutExporter,
    RichTableExporter,
    output_json,
    EXIT_SUCCESS,
    EXIT_NOT_FOUND,
    EXIT_ERROR,
    EXIT_KEYBOARD_INTERRUPT,
)
from job_history.cli.core.output import _JobhistEncoder
from job_history.cli.core.utils import parse_date


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------

class TestContext:
    def test_defaults(self):
        ctx = Context()
        assert ctx.session is None
        assert ctx.machine == "derecho"
        assert ctx.start_date is None
        assert ctx.end_date is None
        assert ctx.group_by == "day"
        assert ctx.output_format == "rich"
        assert ctx.output_dir is None
        assert ctx.verbose is False

    def test_mutable_fields(self):
        ctx = Context()
        ctx.machine = "casper"
        ctx.start_date = date(2026, 1, 1)
        ctx.end_date = date(2026, 1, 31)
        ctx.output_format = "json"
        ctx.verbose = True
        assert ctx.machine == "casper"
        assert ctx.start_date.year == 2026
        assert ctx.output_format == "json"
        assert ctx.verbose is True

    def test_consoles_present(self):
        ctx = Context()
        assert ctx.console is not None
        assert ctx.stderr_console is not None
        # stderr console must target stderr, not stdout
        assert ctx.console is not ctx.stderr_console


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------

class TestExitCodes:
    def test_canonical_values(self):
        assert EXIT_SUCCESS == 0
        assert EXIT_NOT_FOUND == 1
        assert EXIT_ERROR == 2
        assert EXIT_KEYBOARD_INTERRUPT == 130


# ---------------------------------------------------------------------------
# parse_date callback
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_none_passthrough(self):
        assert parse_date(None, None, None) is None

    def test_valid_date(self):
        result = parse_date(None, None, "2026-01-15")
        assert result == date(2026, 1, 15)

    def test_invalid_format_raises(self):
        import click
        with pytest.raises(click.BadParameter):
            parse_date(None, None, "01-15-2026")


# ---------------------------------------------------------------------------
# ExporterRegistry
# ---------------------------------------------------------------------------

class TestExporterRegistry:
    def test_rich_registered(self):
        exporter = ExporterRegistry.resolve("rich")
        assert isinstance(exporter, RichTableExporter)

    def test_json_registered(self):
        exporter = ExporterRegistry.resolve("json")
        assert isinstance(exporter, JSONStdoutExporter)

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError, match="Unknown output format"):
            ExporterRegistry.resolve("xml")

    def test_each_resolve_returns_fresh_instance(self):
        a = ExporterRegistry.resolve("rich")
        b = ExporterRegistry.resolve("rich")
        assert a is not b
        assert type(a) is type(b)

    def test_register_custom_exporter(self):
        class TSVExporter(Exporter):
            def emit(self, envelope, *, ctx, config=None):
                pass

        try:
            ExporterRegistry.register("tsv", TSVExporter)
            assert "tsv" in ExporterRegistry.available()
            assert isinstance(ExporterRegistry.resolve("tsv"), TSVExporter)
        finally:
            ExporterRegistry._registry.pop("tsv", None)

    def test_register_rejects_non_exporter(self):
        class NotAnExporter:
            pass
        with pytest.raises(TypeError):
            ExporterRegistry.register("bad", NotAnExporter)

    def test_available_contains_defaults(self):
        available = ExporterRegistry.available()
        assert "rich" in available
        assert "json" in available


# ---------------------------------------------------------------------------
# JSON encoder + envelope conventions
# ---------------------------------------------------------------------------

class TestJobhistEncoder:
    def test_encodes_date(self):
        result = json.dumps({"d": date(2026, 1, 15)}, cls=_JobhistEncoder)
        assert json.loads(result) == {"d": "2026-01-15"}

    def test_encodes_datetime(self):
        dt = datetime(2026, 1, 15, 12, 30, 45)
        result = json.dumps({"dt": dt}, cls=_JobhistEncoder)
        assert json.loads(result) == {"dt": "2026-01-15T12:30:45"}

    def test_encodes_decimal_as_float(self):
        result = json.dumps({"v": Decimal("3.14")}, cls=_JobhistEncoder)
        assert json.loads(result) == {"v": 3.14}

    def test_encodes_set_as_sorted_list(self):
        result = json.dumps({"s": {3, 1, 2}}, cls=_JobhistEncoder)
        assert json.loads(result) == {"s": [1, 2, 3]}

    def test_unknown_type_raises(self):
        class Weird:
            pass
        with pytest.raises(TypeError):
            json.dumps({"w": Weird()}, cls=_JobhistEncoder)


class TestOutputJson:
    def test_writes_to_stdout(self, capsys):
        envelope = {
            "kind": "jobs_per_user",
            "machine": "derecho",
            "start": date(2026, 1, 1),
            "rows": [{"user": "benkirk", "count": 42}],
        }
        output_json(envelope)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["kind"] == "jobs_per_user"
        assert parsed["start"] == "2026-01-01"
        assert parsed["rows"][0]["count"] == 42

    def test_trailing_newline(self, capsys):
        output_json({"kind": "x"})
        assert capsys.readouterr().out.endswith("\n")


# ---------------------------------------------------------------------------
# JSONStdoutExporter integration
# ---------------------------------------------------------------------------

class TestJSONStdoutExporter:
    def test_emit_preserves_envelope_shape(self, capsys):
        ctx = Context()
        envelope = {
            "kind": "daily_summary",
            "rows": [{"date": date(2026, 3, 1), "jobs": 7}],
        }
        ExporterRegistry.resolve("json").emit(envelope, ctx=ctx)
        out = json.loads(capsys.readouterr().out)
        assert out["kind"] == "daily_summary"
        assert out["rows"][0]["date"] == "2026-03-01"


# ---------------------------------------------------------------------------
# RichTableExporter
# ---------------------------------------------------------------------------

class TestRichTableExporter:
    def test_emit_with_columns(self):
        from rich.console import Console

        buf = io.StringIO()
        ctx = Context()
        ctx.console = Console(file=buf, force_terminal=False, width=120)
        envelope = {
            "kind": "jobs_per_user",
            "columns": [
                {"key": "user", "header": "User", "width": 0, "format": "s"},
                {"key": "count", "header": "Jobs", "width": 0, "format": ""},
            ],
            "rows": [
                {"user": "benkirk", "count": 42},
                {"user": "bdobbins", "count": 17},
            ],
        }
        ExporterRegistry.resolve("rich").emit(envelope, ctx=ctx)
        rendered = buf.getvalue()
        assert "User" in rendered
        assert "Jobs" in rendered
        assert "benkirk" in rendered
        assert "42" in rendered

    def test_emit_without_columns_falls_back_to_dict_keys(self):
        from rich.console import Console

        buf = io.StringIO()
        ctx = Context()
        ctx.console = Console(file=buf, force_terminal=False, width=120)
        envelope = {"rows": [{"a": 1, "b": 2}]}
        ExporterRegistry.resolve("rich").emit(envelope, ctx=ctx)
        rendered = buf.getvalue()
        assert "a" in rendered
        assert "b" in rendered

    def test_emit_handles_empty_rows(self):
        from rich.console import Console

        buf = io.StringIO()
        ctx = Context()
        ctx.console = Console(file=buf, force_terminal=False, width=120)
        envelope = {
            "columns": [{"key": "user", "header": "User", "width": 0, "format": "s"}],
            "rows": [],
        }
        ExporterRegistry.resolve("rich").emit(envelope, ctx=ctx)
        # Should render the header but no data rows; no exception.
        assert "User" in buf.getvalue()


# ---------------------------------------------------------------------------
# BaseCommand wiring
# ---------------------------------------------------------------------------

class TestBaseCommand:
    def test_handle_exception_returns_error_code(self, capsys):
        from job_history.cli.core import BaseCommand

        class DummyCommand(BaseCommand):
            def execute(self, **kwargs):
                return 0

        ctx = Context()
        cmd = DummyCommand(ctx)
        code = cmd.handle_exception(RuntimeError("boom"))
        assert code == EXIT_ERROR
        # The error message goes to ctx.stderr_console, which writes to
        # real sys.stderr; capsys captures it.
        captured = capsys.readouterr()
        assert "boom" in captured.err
