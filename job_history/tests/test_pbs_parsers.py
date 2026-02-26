"""Unit tests for PBS log parsers."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from job_history.pbs_parsers import (
    parse_pbs_time,
    parse_pbs_memory_kb,
    parse_pbs_memory_gb,
    parse_pbs_timestamp,
    parse_select_string,
    infer_types_from_queue,
    parse_pbs_record,
)


class TestPbsTime:
    """Tests for parse_pbs_time()."""

    def test_parse_simple_time(self):
        """Parse basic HH:MM:SS time."""
        assert parse_pbs_time("00:14:18") == 858

    def test_parse_multidigit_hours(self):
        """Parse time with hours > 9."""
        assert parse_pbs_time("06:42:05") == 24125

    def test_parse_large_time(self):
        """Parse very large time values."""
        assert parse_pbs_time("57:17:49") == 206269

    def test_parse_zero_time(self):
        """Parse zero time."""
        assert parse_pbs_time("00:00:00") == 0

    def test_parse_none(self):
        """Return None for None input."""
        assert parse_pbs_time(None) is None

    def test_parse_empty(self):
        """Return None for empty string."""
        assert parse_pbs_time("") is None

    def test_parse_invalid_format(self):
        """Return None for invalid format."""
        assert parse_pbs_time("invalid") is None
        assert parse_pbs_time("12:34") is None  # Missing seconds


class TestPbsMemoryKb:
    """Tests for parse_pbs_memory_kb()."""

    def test_parse_simple_kb(self):
        """Parse basic kilobyte value."""
        assert parse_pbs_memory_kb("172600kb") == 176742400

    def test_parse_1mb(self):
        """Parse 1MB in kilobytes."""
        assert parse_pbs_memory_kb("1024kb") == 1048576

    def test_parse_large_value(self):
        """Parse large memory value."""
        assert parse_pbs_memory_kb("233791056kb") == 239402041344

    def test_parse_uppercase(self):
        """Handle uppercase suffix."""
        assert parse_pbs_memory_kb("1024KB") == 1048576

    def test_parse_none(self):
        """Return None for None input."""
        assert parse_pbs_memory_kb(None) is None

    def test_parse_empty(self):
        """Return None for empty string."""
        assert parse_pbs_memory_kb("") is None

    def test_parse_invalid(self):
        """Return None for invalid format."""
        assert parse_pbs_memory_kb("invalid") is None


class TestPbsMemoryGb:
    """Tests for parse_pbs_memory_gb()."""

    def test_parse_simple_gb(self):
        """Parse basic gigabyte value."""
        result = parse_pbs_memory_gb("235gb")
        expected = int(235 * 1024 * 1024 * 1024)
        assert result == expected

    def test_parse_fractional_gb(self):
        """Parse fractional gigabyte value."""
        result = parse_pbs_memory_gb("150.5gb")
        expected = int(150.5 * 1024 * 1024 * 1024)
        assert result == expected

    def test_parse_uppercase_suffix(self):
        """Handle uppercase GB suffix."""
        result = parse_pbs_memory_gb("100GB")
        expected = int(100 * 1024 * 1024 * 1024)
        assert result == expected

    def test_parse_short_suffix(self):
        """Handle short G suffix."""
        result = parse_pbs_memory_gb("150G")
        expected = int(150 * 1024 * 1024 * 1024)
        assert result == expected

    def test_parse_none(self):
        """Return None for None input."""
        assert parse_pbs_memory_gb(None) is None

    def test_parse_empty(self):
        """Return None for empty string."""
        assert parse_pbs_memory_gb("") is None

    def test_parse_invalid(self):
        """Return None for invalid format."""
        assert parse_pbs_memory_gb("invalid") is None


class TestPbsTimestamp:
    """Tests for parse_pbs_timestamp()."""

    def test_parse_integer_timestamp(self):
        """Parse Unix timestamp as integer."""
        result = parse_pbs_timestamp(1769670016)
        expected = datetime(2026, 1, 29, 7, 0, 16, tzinfo=timezone.utc)
        assert result == expected

    def test_parse_string_timestamp(self):
        """Parse Unix timestamp as string."""
        result = parse_pbs_timestamp("1769670016")
        expected = datetime(2026, 1, 29, 7, 0, 16, tzinfo=timezone.utc)
        assert result == expected

    def test_parse_none(self):
        """Return None for None input."""
        assert parse_pbs_timestamp(None) is None

    def test_parse_empty(self):
        """Return None for empty string."""
        assert parse_pbs_timestamp("") is None

    def test_parse_invalid(self):
        """Return None for invalid format."""
        assert parse_pbs_timestamp("invalid") is None


class TestSelectString:
    """Tests for parse_select_string()."""

    def test_parse_full_select(self):
        """Parse select string with all fields."""
        select = "1:ncpus=128:mpiprocs=128:mem=235GB:ompthreads=1:cpu_type=genoa"
        result = parse_select_string(select)
        assert result == {
            "mpiprocs": 128,
            "ompthreads": 1,
            "cpu_type": "genoa"
        }

    def test_parse_with_gpu_type(self):
        """Parse select string with GPU type."""
        select = "1:ncpus=64:mpiprocs=1:gpu_type=a100:mem=100GB"
        result = parse_select_string(select)
        assert result == {
            "mpiprocs": 1,
            "gpu_type": "a100"
        }

    def test_parse_minimal_select(self):
        """Parse select string with minimal fields."""
        select = "1:ncpus=4:mem=70GB:ompthreads=1"
        result = parse_select_string(select)
        assert result == {"ompthreads": 1}

    def test_parse_multinode_select(self):
        """Parse multi-node select string."""
        select = "5:ncpus=128:mpiprocs=128:mem=150G:ompthreads=1"
        result = parse_select_string(select)
        assert result == {
            "mpiprocs": 128,
            "ompthreads": 1
        }

    def test_parse_empty(self):
        """Return empty dict for empty string."""
        assert parse_select_string("") == {}

    def test_parse_none(self):
        """Return empty dict for None."""
        assert parse_select_string(None) == {}


class TestInferTypes:
    """Tests for infer_types_from_queue()."""

    def test_infer_a100_gpu(self):
        """Infer a100 GPU type from queue name."""
        result = infer_types_from_queue("a100", "derecho")
        assert result == {"gputype": "a100"}

    def test_infer_h100_gpu(self):
        """Infer h100 GPU type from queue name."""
        result = infer_types_from_queue("h100", "casper")
        assert result == {"gputype": "h100"}

    def test_infer_nvgpu_as_v100(self):
        """Infer V100 GPU type from nvgpu queue."""
        result = infer_types_from_queue("nvgpu", "casper")
        assert result == {"gputype": "v100"}

    def test_infer_cpu_derecho(self):
        """Infer Milan CPU type for derecho cpu queue."""
        result = infer_types_from_queue("cpu", "derecho")
        assert result == {"cputype": "milan"}

    def test_infer_cpu_casper(self):
        """Don't infer CPU type for casper (mixed types)."""
        result = infer_types_from_queue("cpu", "casper")
        assert result == {}

    def test_infer_unknown_queue(self):
        """Handle unknown queue gracefully."""
        result = infer_types_from_queue("unknown", "derecho")
        # Should fall back to machine default
        assert result == {"cputype": "milan"}


class TestParsePbsRecord:
    """Tests for parse_pbs_record()."""

    def test_account_quote_removal(self):
        """Verify quotes are stripped from account field."""
        # Mock minimal PbsRecord
        class MockRecord:
            id = "4779496.desched1"
            short_id = "4779496"
            account = '"UCSD0047"'  # With quotes
            user = "testuser"
            queue = "cpu"
            jobname = "testjob"
            ctime = "1769670000"
            etime = "1769670000"
            start = "1769670010"
            end = "1769670020"
            Exit_status = "0"
            run_count = "1"
            Resource_List = {}
            resources_used = {}

        record = MockRecord()
        result = parse_pbs_record(record, "derecho")

        assert result["account"] == "UCSD0047"  # Quotes removed

    def test_parse_full_record(self):
        """Parse a complete PBS record."""
        class MockRecord:
            id = "4779496.desched1"
            short_id = "4779496"
            account = '"UCSD0047"'
            user = "nghido"
            queue = "cpu"
            jobname = "VerifyObsDA"
            ctime = "1769669942"
            etime = "1769669942"
            start = "1769669984"
            end = "1769670016"
            Exit_status = "0"
            run_count = "1"
            Resource_List = {
                "mem": "235gb",
                "mpiprocs": "128",
                "ncpus": "128",
                "ngpus": "0",
                "nodect": "1",
                "select": "1:ncpus=128:mpiprocs=128:mem=235GB:ompthreads=1",
                "walltime": "00:20:20",
                "preempt_targets": "QUEUE=pcpu"
            }
            resources_used = {
                "cpupercent": "31",
                "cput": "00:00:07",
                "mem": "172600kb",
                "vmem": "148112kb",
                "walltime": "00:00:24"
            }

        record = MockRecord()
        result = parse_pbs_record(record, "derecho")

        # Check basic fields
        assert result["job_id"] == "4779496.desched1"
        assert result["short_id"] == 4779496
        assert result["account"] == "UCSD0047"
        assert result["user"] == "nghido"
        assert result["queue"] == "cpu"
        assert result["name"] == "VerifyObsDA"
        assert result["status"] == "0"

        # Check timestamps
        assert result["submit"] == datetime(2026, 1, 29, 6, 59, 2, tzinfo=timezone.utc)
        assert result["start"] == datetime(2026, 1, 29, 6, 59, 44, tzinfo=timezone.utc)
        assert result["end"] == datetime(2026, 1, 29, 7, 0, 16, tzinfo=timezone.utc)

        # Check time fields (in seconds)
        assert result["walltime"] == 1220  # 00:20:20
        assert result["elapsed"] == 24     # 00:00:24
        assert result["cputime"] == 7      # 00:00:07

        # Check resource allocation
        assert result["numcpus"] == 128
        assert result["numgpus"] == 0
        assert result["numnodes"] == 1
        assert result["mpiprocs"] == 128
        assert result["ompthreads"] == 1

        # Check memory (in bytes)
        assert result["reqmem"] == int(235 * 1024**3)
        assert result["memory"] == 172600 * 1024
        assert result["vmemory"] == 148112 * 1024

        # Check types (should infer from queue)
        assert result["cputype"] == "milan"  # Derecho default
        assert result["gputype"] is None

        # Check other fields
        assert result["resources"] == "1:ncpus=128:mpiprocs=128:mem=235GB:ompthreads=1"
        assert result["ptargets"] == "QUEUE=pcpu"
        assert result["cpupercent"] == 31.0
        assert result["count"] == 1

    def test_parse_with_cpu_type_in_select(self):
        """Parse record with cpu_type in select string."""
        class MockRecord:
            id = "123.desched1"
            short_id = "123"
            account = '"TEST0001"'
            user = "testuser"
            queue = "cpu"
            jobname = "test"
            ctime = "1769670000"
            etime = "1769670000"
            start = "1769670010"
            end = "1769670020"
            Exit_status = "0"
            run_count = "1"
            Resource_List = {
                "select": "1:ncpus=128:cpu_type=genoa:mpiprocs=128"
            }
            resources_used = {}

        record = MockRecord()
        result = parse_pbs_record(record, "derecho")

        # Should use cpu_type from select, not machine default
        assert result["cputype"] == "genoa"
        assert result["mpiprocs"] == 128


    def test_parse_with_priority(self):
        """Parse record with job_priority in Resource_List."""
        class MockRecord:
            id = "123.desched1"
            short_id = "123"
            account = '"TEST0001"'
            user = "testuser"
            queue = "cpu"
            jobname = "test"
            ctime = "1769670000"
            etime = "1769670000"
            start = "1769670010"
            end = "1769670020"
            Exit_status = "0"
            run_count = "1"
            Resource_List = {
                "job_priority": "premium",
                "select": "1:ncpus=1"
            }
            resources_used = {}

        record = MockRecord()
        result = parse_pbs_record(record, "derecho")

        assert result["priority"] == "premium"

class TestIntegrationWithSampleData:
    """Integration tests using actual sample data."""

    @pytest.mark.skipif(
        not Path("./data/sample_pbs_logs/derecho/20260129").exists(),
        reason="Sample data not available"
    )
    def test_parse_sample_logs(self):
        """Parse sample PBS logs and verify record count."""
        import pbsparse
        from job_history.pbs_parsers import parse_pbs_record

        log_path = "./data/sample_pbs_logs/derecho/20260129"
        records = list(pbsparse.get_pbs_records(log_path, type_filter="E"))

        assert len(records) > 0, "Should find some End records"

        # Parse all records
        parsed = []
        for pbs_record in records:
            job_dict = parse_pbs_record(pbs_record, "derecho")
            parsed.append(job_dict)

        # Verify all have required fields
        for job in parsed:
            assert job["job_id"], "All jobs should have job_id"
            assert job["user"], "All jobs should have user"
            assert job["account"], "All jobs should have account"
            assert '"' not in job["account"], "Account should not have quotes"

        # Verify at least some have timestamps
        jobs_with_submit = sum(1 for j in parsed if j["submit"] is not None)
        assert jobs_with_submit > 0, "Should parse some submit timestamps"

    @pytest.mark.skipif(
        not Path("./data/sample_pbs_logs/derecho/20260129").exists(),
        reason="Sample data not available"
    )
    def test_fetch_jobs_iterator(self):
        """Test the full iterator interface."""
        from job_history.pbs_read_logs import fetch_jobs_from_pbs_logs

        log_dir = "./data/sample_pbs_logs/derecho"
        jobs = list(fetch_jobs_from_pbs_logs(
            log_dir=log_dir,
            machine="derecho",
            date="2026-01-29"
        ))

        assert len(jobs) > 0, "Should fetch some jobs"

        # Verify structure
        for job in jobs:
            assert isinstance(job, dict)
            assert "job_id" in job
            assert "user" in job
            assert "submit" in job
