"""Smoke tests for ``jobhist sync`` registered on the new entry point."""

import pytest
from click.testing import CliRunner

from job_history.cli.cmds.jobhist import cli


class TestSyncRegistration:
    def test_sync_present_on_new_entry_point(self):
        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "sync" in result.output

    def test_sync_help_renders(self):
        result = CliRunner().invoke(cli, ["sync", "--help"])
        assert result.exit_code == 0
        # Spot-check the mutually-exclusive mode flags survived.
        for flag in ("--upsert", "--incremental", "--resummarize", "--recalculate", "--dry-run"):
            assert flag in result.output

    def test_sync_mutually_exclusive_mode_flags(self):
        # No DB connection needed — the validator short-circuits before any IO.
        result = CliRunner().invoke(cli, [
            "sync", "-m", "derecho", "--upsert", "--recalculate",
        ])
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output.lower()
