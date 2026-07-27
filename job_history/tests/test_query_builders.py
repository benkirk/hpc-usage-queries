"""Tests for query builder utilities."""

import pytest
from sqlalchemy import func
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.dialects.postgresql import dialect as pg_dialect
from job_history.queries.builders import PeriodGrouper, ResourceTypeResolver


def _sql(expr, dialect_cls):
    """Compile *expr* against *dialect_cls* and return the SQL string."""
    return str(expr.compile(dialect=dialect_cls()))


class TestPeriodGrouper:
    """Tests for the PeriodGrouper class."""

    def test_get_period_func_day(self):
        """Test day period function compiles correctly for both dialects."""
        from job_history.database import Job

        period_func = PeriodGrouper.get_period_func('day', Job.end)

        assert 'strftime' in _sql(period_func, sqlite_dialect)
        assert 'to_char'  in _sql(period_func, pg_dialect)

    def test_get_period_func_month(self):
        """Test month period function compiles correctly for both dialects."""
        from job_history.database import Job

        period_func = PeriodGrouper.get_period_func('month', Job.end)

        assert 'strftime' in _sql(period_func, sqlite_dialect)
        assert 'to_char'  in _sql(period_func, pg_dialect)

    def test_get_period_func_quarter(self):
        """Test quarter returns a SQL expression for YYYY-Q# for both dialects."""
        from job_history.database import Job

        period_func = PeriodGrouper.get_period_func('quarter', Job.end)

        sqlite_sql = _sql(period_func, sqlite_dialect)
        assert 'strftime' in sqlite_sql
        assert 'CAST' in sqlite_sql

        pg_sql = _sql(period_func, pg_dialect)
        assert 'to_char' in pg_sql
        assert 'CAST' in pg_sql

    def test_get_period_func_year(self):
        """Test year period function compiles correctly for both dialects."""
        from job_history.database import Job

        period_func = PeriodGrouper.get_period_func('year', Job.end)

        assert 'strftime' in _sql(period_func, sqlite_dialect)
        assert 'to_char'  in _sql(period_func, pg_dialect)

    def test_get_period_func_invalid(self):
        """Test that invalid period raises ValueError."""
        from job_history.database import Job

        with pytest.raises(ValueError) as exc_info:
            PeriodGrouper.get_period_func('invalid', Job.end)

        assert "Invalid period" in str(exc_info.value)
        assert "invalid" in str(exc_info.value)

    def test_aggregate_quarters_simple(self):
        """Test basic quarter aggregation with single field."""
        monthly = [
            {'period': '2025-01', 'job_count': 10},
            {'period': '2025-02', 'job_count': 15},
            {'period': '2025-03', 'job_count': 20},
            {'period': '2025-04', 'job_count': 5},
        ]

        result = PeriodGrouper.aggregate_quarters(monthly, 'job_count')

        assert len(result) == 2
        assert result[0]['period'] == '2025-Q1'
        assert result[0]['job_count'] == 45  # 10+15+20
        assert result[1]['period'] == '2025-Q2'
        assert result[1]['job_count'] == 5

    def test_aggregate_quarters_year_boundary(self):
        """Test quarter aggregation across year boundaries."""
        monthly = [
            {'period': '2024-12', 'job_count': 100},
            {'period': '2025-01', 'job_count': 150},
            {'period': '2025-02', 'job_count': 200},
        ]

        result = PeriodGrouper.aggregate_quarters(monthly, 'job_count')

        assert len(result) == 2
        assert result[0]['period'] == '2024-Q4'
        assert result[0]['job_count'] == 100
        assert result[1]['period'] == '2025-Q1'
        assert result[1]['job_count'] == 350  # 150+200

    def test_aggregate_quarters_all_quarters(self):
        """Test aggregation produces all 4 quarters correctly."""
        monthly = [
            {'period': '2025-01', 'job_count': 1},   # Q1
            {'period': '2025-02', 'job_count': 2},   # Q1
            {'period': '2025-03', 'job_count': 3},   # Q1
            {'period': '2025-04', 'job_count': 4},   # Q2
            {'period': '2025-05', 'job_count': 5},   # Q2
            {'period': '2025-06', 'job_count': 6},   # Q2
            {'period': '2025-07', 'job_count': 7},   # Q3
            {'period': '2025-08', 'job_count': 8},   # Q3
            {'period': '2025-09', 'job_count': 9},   # Q3
            {'period': '2025-10', 'job_count': 10},  # Q4
            {'period': '2025-11', 'job_count': 11},  # Q4
            {'period': '2025-12', 'job_count': 12},  # Q4
        ]

        result = PeriodGrouper.aggregate_quarters(monthly, 'job_count')

        assert len(result) == 4
        assert result[0] == {'period': '2025-Q1', 'job_count': 6}   # 1+2+3
        assert result[1] == {'period': '2025-Q2', 'job_count': 15}  # 4+5+6
        assert result[2] == {'period': '2025-Q3', 'job_count': 24}  # 7+8+9
        assert result[3] == {'period': '2025-Q4', 'job_count': 33}  # 10+11+12

    def test_aggregate_quarters_with_single_grouping_field(self):
        """Test quarter aggregation with one grouping field."""
        monthly = [
            {'period': '2025-01', 'user': 'alice', 'job_count': 10},
            {'period': '2025-02', 'user': 'alice', 'job_count': 15},
            {'period': '2025-01', 'user': 'bob', 'job_count': 5},
            {'period': '2025-02', 'user': 'bob', 'job_count': 7},
        ]

        result = PeriodGrouper.aggregate_quarters(
            monthly, 'job_count', grouping_fields=['user']
        )

        assert len(result) == 2
        # Results should be sorted by period, then by grouping fields
        alice_result = [r for r in result if r['user'] == 'alice'][0]
        bob_result = [r for r in result if r['user'] == 'bob'][0]

        assert alice_result['period'] == '2025-Q1'
        assert alice_result['job_count'] == 25  # 10+15
        assert bob_result['period'] == '2025-Q1'
        assert bob_result['job_count'] == 12  # 5+7

    def test_aggregate_quarters_with_multiple_grouping_fields(self):
        """Test quarter aggregation with multiple grouping fields."""
        monthly = [
            {'period': '2025-01', 'user': 'alice', 'account': 'A1', 'job_count': 10},
            {'period': '2025-02', 'user': 'alice', 'account': 'A1', 'job_count': 15},
            {'period': '2025-01', 'user': 'alice', 'account': 'A2', 'job_count': 5},
            {'period': '2025-01', 'user': 'bob', 'account': 'A1', 'job_count': 20},
        ]

        result = PeriodGrouper.aggregate_quarters(
            monthly, 'job_count', grouping_fields=['user', 'account']
        )

        assert len(result) == 3
        alice_a1 = [r for r in result if r['user'] == 'alice' and r['account'] == 'A1'][0]
        alice_a2 = [r for r in result if r['user'] == 'alice' and r['account'] == 'A2'][0]
        bob_a1 = [r for r in result if r['user'] == 'bob' and r['account'] == 'A1'][0]

        assert alice_a1['job_count'] == 25  # 10+15
        assert alice_a2['job_count'] == 5
        assert bob_a1['job_count'] == 20

    def test_aggregate_quarters_empty_input(self):
        """Test aggregation with empty input list."""
        result = PeriodGrouper.aggregate_quarters([], 'job_count')
        assert result == []

    def test_aggregate_quarters_missing_period(self):
        """Test aggregation skips rows without period field."""
        monthly = [
            {'period': '2025-01', 'job_count': 10},
            {'job_count': 20},  # Missing period
            {'period': '2025-02', 'job_count': 15},
        ]

        result = PeriodGrouper.aggregate_quarters(monthly, 'job_count')

        assert len(result) == 1
        assert result[0]['job_count'] == 25  # Only 10+15, skips 20

    def test_aggregate_quarters_distinct_simple(self):
        """Test distinct entity aggregation into quarters."""
        monthly = [
            ('2025-01', 'alice'),
            ('2025-02', 'alice'),  # Same user, shouldn't double count
            ('2025-02', 'bob'),
            ('2025-04', 'charlie'),
        ]

        result = PeriodGrouper.aggregate_quarters_distinct(monthly, 'user_count')

        assert len(result) == 2
        assert result[0]['period'] == '2025-Q1'
        assert result[0]['user_count'] == 2  # alice, bob
        assert result[1]['period'] == '2025-Q2'
        assert result[1]['user_count'] == 1  # charlie

    def test_aggregate_quarters_distinct_all_quarters(self):
        """Test distinct aggregation across all quarters."""
        monthly = [
            ('2025-01', 'alice'),  # Q1
            ('2025-03', 'bob'),    # Q1
            ('2025-04', 'charlie'), # Q2
            ('2025-07', 'david'),  # Q3
            ('2025-10', 'eve'),    # Q4
        ]

        result = PeriodGrouper.aggregate_quarters_distinct(monthly, 'user_count')

        assert len(result) == 4
        assert result[0] == {'period': '2025-Q1', 'user_count': 2}  # alice, bob
        assert result[1] == {'period': '2025-Q2', 'user_count': 1}  # charlie
        assert result[2] == {'period': '2025-Q3', 'user_count': 1}  # david
        assert result[3] == {'period': '2025-Q4', 'user_count': 1}  # eve

    def test_aggregate_quarters_distinct_duplicate_same_month(self):
        """Test that duplicates within same month are de-duplicated."""
        monthly = [
            ('2025-01', 'alice'),
            ('2025-01', 'alice'),  # Duplicate same month
            ('2025-01', 'alice'),  # Duplicate same month
            ('2025-01', 'bob'),
        ]

        result = PeriodGrouper.aggregate_quarters_distinct(monthly, 'user_count')

        assert len(result) == 1
        assert result[0]['period'] == '2025-Q1'
        assert result[0]['user_count'] == 2  # alice, bob (alice counted once)

    def test_aggregate_quarters_distinct_empty_input(self):
        """Test distinct aggregation with empty input."""
        result = PeriodGrouper.aggregate_quarters_distinct([], 'user_count')
        assert result == []

    def test_aggregate_quarters_distinct_null_values(self):
        """Test distinct aggregation skips null values."""
        monthly = [
            ('2025-01', 'alice'),
            ('2025-01', None),  # Null entity
            (None, 'bob'),      # Null month
            ('2025-02', 'charlie'),
        ]

        result = PeriodGrouper.aggregate_quarters_distinct(monthly, 'user_count')

        assert len(result) == 1
        assert result[0]['period'] == '2025-Q1'
        assert result[0]['user_count'] == 2  # alice, charlie (nulls skipped)


class TestResourceTypeResolver:
    """Tests for the ResourceTypeResolver class."""

    def test_resolve_cpu_derecho(self):
        """Test CPU resource type resolution for Derecho."""
        from job_history.database import JobCharge

        queues, hours_field = ResourceTypeResolver.resolve('cpu', 'derecho', JobCharge)

        assert 'cpu' in queues
        assert 'cpudev' in queues
        # hours_field should reference JobCharge.cpu_hours
        assert 'cpu_hours' in str(hours_field)

    def test_resolve_gpu_derecho(self):
        """Test GPU resource type resolution for Derecho."""
        from job_history.database import JobCharge

        queues, hours_field = ResourceTypeResolver.resolve('gpu', 'derecho', JobCharge)

        assert 'gpu' in queues
        assert 'gpudev' in queues
        assert 'pgpu' in queues
        # hours_field should reference JobCharge.gpu_hours
        assert 'gpu_hours' in str(hours_field)

    def test_resolve_all_derecho(self):
        """Test 'all' resource type resolution for Derecho."""
        from job_history.database import JobCharge

        queues, hours_field = ResourceTypeResolver.resolve('all', 'derecho', JobCharge)

        # Should include both CPU and GPU queues
        assert 'cpu' in queues
        assert 'cpudev' in queues
        assert 'gpu' in queues
        assert 'gpudev' in queues
        assert 'pgpu' in queues

        # hours_field should be sum of cpu_hours and gpu_hours
        field_str = str(hours_field)
        assert 'cpu_hours' in field_str
        assert 'gpu_hours' in field_str
        assert 'coalesce' in field_str.lower()

    def test_resolve_cpu_casper(self):
        """Test CPU resource type resolution for Casper."""
        from job_history.database import JobCharge

        queues, hours_field = ResourceTypeResolver.resolve('cpu', 'casper', JobCharge)

        # Casper CPU queues
        assert 'htc' in queues
        assert 'gdex' in queues
        assert 'largemem' in queues
        assert 'vis' in queues
        assert 'rda' in queues

    def test_resolve_gpu_casper(self):
        """Test GPU resource type resolution for Casper."""
        from job_history.database import JobCharge

        queues, hours_field = ResourceTypeResolver.resolve('gpu', 'casper', JobCharge)

        # Casper GPU queues
        assert 'nvgpu' in queues or 'gpgpu' in queues or 'a100' in queues

    def test_resolve_invalid_resource_type(self):
        """Test that invalid resource type raises ValueError."""
        from job_history.database import JobCharge

        with pytest.raises(ValueError) as exc_info:
            ResourceTypeResolver.resolve('invalid', 'derecho', JobCharge)

        assert "Invalid resource_type" in str(exc_info.value)
        assert "invalid" in str(exc_info.value)
        assert "cpu" in str(exc_info.value).lower()
        assert "gpu" in str(exc_info.value).lower()

    def test_resolve_case_insensitive_machine(self):
        """Test that machine names are case-insensitive."""
        from job_history.database import JobCharge

        # Test uppercase
        queues_upper, _ = ResourceTypeResolver.resolve('cpu', 'DERECHO', JobCharge)
        # Test lowercase
        queues_lower, _ = ResourceTypeResolver.resolve('cpu', 'derecho', JobCharge)

        # Should produce same results (case-insensitive in QueryConfig.get_cpu_queues)
        assert queues_upper == queues_lower or set(queues_upper) == set(queues_lower)


class TestGlobTranslation:
    """Pure translation functions.

    Mirrors fs_scans/tests/test_fs_scans.py::test_glob_to_posix_regex_escapes_metacharacters,
    plus the LIKE-escaping this copy adds deliberately.
    """

    def test_glob_to_posix_regex_escapes_metacharacters(self):
        from job_history.queries.builders import glob_to_posix_regex

        assert glob_to_posix_regex("a.b+c") == r"^a\.b\+c$"
        assert glob_to_posix_regex("*.log") == r"^.*\.log$"
        assert glob_to_posix_regex("data_?") == r"^data_.$"
        # `_` and `%` are not regex metacharacters — untouched on this path.
        assert glob_to_posix_regex("wrf_100%") == r"^wrf_100%$"

    def test_glob_to_sql_like_escapes_like_wildcards(self):
        """The divergence from fs_scans: a literal `_` must not wildcard."""
        from job_history.queries.builders import glob_to_sql_like

        assert glob_to_sql_like("wrf_*") == "wrf!_%"
        assert glob_to_sql_like("*100%*") == "%100!%%"
        assert glob_to_sql_like("a?b") == "a_b"
        assert glob_to_sql_like("bang!") == "bang!!"
        assert glob_to_sql_like("cesm_b1850") == "cesm!_b1850"


class TestGlobMatchClause:
    """Dialect dispatch.

    The suite runs on SQLite, so the PostgreSQL branches are only reachable by
    compiling the expression offline against postgresql.dialect() and asserting
    on the emitted SQL plus bind values.
    """

    @staticmethod
    def _compile(expr, dialect_cls):
        compiled = expr.compile(dialect=dialect_cls())
        return str(compiled), compiled.params

    def test_returns_none_for_empty_patterns(self):
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        assert glob_match_clause(Job.name, (), dialect="sqlite") is None
        assert glob_match_clause(Job.name, ("",), dialect="sqlite") is None

    def test_sqlite_case_sensitive_uses_glob_verbatim(self):
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        sql, params = self._compile(
            glob_match_clause(Job.name, ["wrf_*"], dialect="sqlite"),
            sqlite_dialect,
        )
        assert "GLOB" in sql and "LIKE" not in sql
        # GLOB takes the glob unmodified — no `_`/`%` translation at all.
        assert list(params.values()) == ["wrf_*"]

    def test_postgres_case_sensitive_uses_anchored_regex(self):
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        sql, params = self._compile(
            glob_match_clause(Job.name, ["*scratch*", "tmp?"], dialect="postgresql"),
            pg_dialect,
        )
        assert " ~ " in sql
        assert "GLOB" not in sql and "LIKE" not in sql
        assert sorted(params.values()) == sorted(["^.*scratch.*$", "^tmp.$"])

    def test_postgres_ignore_case_uses_ilike(self):
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        sql, params = self._compile(
            glob_match_clause(Job.name, ["*TMP*"], dialect="postgresql",
                              ignore_case=True),
            pg_dialect,
        )
        assert "ILIKE" in sql
        assert "ESCAPE '!'" in sql
        assert list(params.values()) == ["%TMP%"]

    def test_sqlite_ignore_case_lowers_both_sides(self):
        """SQLAlchemy's portable ilike(): no hand-rolled dialect branch needed."""
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        sql, params = self._compile(
            glob_match_clause(Job.name, ["*TMP*"], dialect="sqlite",
                              ignore_case=True),
            sqlite_dialect,
        )
        assert "lower(jobs.name) LIKE lower(" in sql
        assert "ESCAPE '!'" in sql
        assert list(params.values()) == ["%TMP%"]

    def test_ignore_case_escapes_underscore_on_both_dialects(self):
        """Regression guard for the `_` leak inherited from fs_scans.

        Also pins the escape character: `!` renders byte-identically compiled
        offline and against a live connection, whereas a backslash would be
        doubled by the PG compiler while _backslash_escapes is True — making
        an offline assertion green against SQL production never emits.
        """
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        for dialect_name, dialect_cls in (("sqlite", sqlite_dialect),
                                          ("postgresql", pg_dialect)):
            sql, params = self._compile(
                glob_match_clause(Job.name, ["cesm_b*"], dialect=dialect_name,
                                  ignore_case=True),
                dialect_cls,
            )
            assert list(params.values()) == ["cesm!_b%"]
            assert "ESCAPE '!'" in sql

    def test_multiple_patterns_are_ored(self):
        from job_history.database import Job
        from job_history.queries.builders import glob_match_clause

        sql, _ = self._compile(
            glob_match_clause(Job.name, ["a*", "b*"], dialect="sqlite"),
            sqlite_dialect,
        )
        assert " OR " in sql
