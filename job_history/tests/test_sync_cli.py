"""Tests for jobhist sync CLI date options."""

from datetime import date, timedelta

import pytest
import click

from job_history.sync.cli import parse_last_spec, validate_dates


class TestParseLastSpec:
    """Tests for parse_last_spec()."""

    def test_days_suffix(self):
        assert parse_last_spec("3d") == 3

    def test_days_suffix_uppercase(self):
        assert parse_last_spec("3D") == 3

    def test_no_suffix(self):
        assert parse_last_spec("7") == 7

    def test_single_day(self):
        assert parse_last_spec("1d") == 1

    def test_large_value(self):
        assert parse_last_spec("365d") == 365

    def test_invalid_non_numeric(self):
        with pytest.raises(click.BadParameter, match="--last must be in the form"):
            parse_last_spec("foo")

    def test_invalid_float(self):
        with pytest.raises(click.BadParameter):
            parse_last_spec("3.5d")

    def test_zero(self):
        with pytest.raises(click.BadParameter, match="must be >= 1"):
            parse_last_spec("0d")

    def test_negative(self):
        with pytest.raises(click.BadParameter, match="must be >= 1"):
            parse_last_spec("-1d")


class TestValidateDates:
    """Tests for validate_dates() mutual-exclusion rules."""

    def test_today_with_date_conflicts(self):
        with pytest.raises(click.BadParameter, match="--today cannot be combined"):
            validate_dates(date="2026-01-01", start=None, end=None, today_flag=True)

    def test_today_with_start_conflicts(self):
        with pytest.raises(click.BadParameter, match="--today cannot be combined"):
            validate_dates(date=None, start="2026-01-01", end=None, today_flag=True)

    def test_today_with_end_conflicts(self):
        with pytest.raises(click.BadParameter, match="--today cannot be combined"):
            validate_dates(date=None, start=None, end="2026-01-31", today_flag=True)

    def test_today_with_last_conflicts(self):
        with pytest.raises(click.BadParameter, match="--today cannot be combined"):
            validate_dates(date=None, start=None, end=None, today_flag=True, last="3d")

    def test_last_with_date_conflicts(self):
        with pytest.raises(click.BadParameter, match="--last cannot be combined"):
            validate_dates(date="2026-01-01", start=None, end=None, last="3d")

    def test_last_with_start_conflicts(self):
        with pytest.raises(click.BadParameter, match="--last cannot be combined"):
            validate_dates(date=None, start="2026-01-01", end=None, last="3d")

    def test_last_with_end_conflicts(self):
        with pytest.raises(click.BadParameter, match="--last cannot be combined"):
            validate_dates(date=None, start=None, end="2026-01-31", last="3d")

    def test_date_with_start_conflicts(self):
        with pytest.raises(click.BadParameter, match="Cannot use --date with --start/--end"):
            validate_dates(date="2026-01-01", start="2026-01-01", end=None)

    def test_today_alone_is_valid(self):
        validate_dates(date=None, start=None, end=None, today_flag=True)  # no exception

    def test_last_alone_is_valid(self):
        validate_dates(date=None, start=None, end=None, last="3d")  # no exception

    def test_date_alone_is_valid(self):
        validate_dates(date="2026-01-01", start=None, end=None)  # no exception

    def test_start_end_is_valid(self):
        validate_dates(date=None, start="2026-01-01", end="2026-01-31")  # no exception


class TestLastResolution:
    """Verify --last N resolves to the correct start/end window."""

    def test_last_1d_is_today_only(self):
        today = date.today()
        n = parse_last_spec("1d")
        start = (today - timedelta(days=n - 1)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        assert start == end == today.strftime("%Y-%m-%d")

    def test_last_3d_window(self):
        today = date.today()
        n = parse_last_spec("3d")
        start = (today - timedelta(days=n - 1)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        assert start == (today - timedelta(days=2)).strftime("%Y-%m-%d")
        assert end == today.strftime("%Y-%m-%d")

    def test_last_7d_window(self):
        today = date.today()
        n = parse_last_spec("7d")
        start = (today - timedelta(days=n - 1)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        assert start == (today - timedelta(days=6)).strftime("%Y-%m-%d")
        assert end == today.strftime("%Y-%m-%d")
