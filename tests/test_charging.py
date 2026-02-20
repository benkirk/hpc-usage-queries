"""Tests for charging calculation functions."""

import pytest

from job_history.charging import derecho_charge, casper_charge


class TestDerechoCharging:
    """Tests for Derecho charging calculations."""

    def test_cpu_production_queue(self, derecho_cpu_job):
        """CPU production: elapsed * numnodes * 128 / 3600."""
        result = derecho_charge(derecho_cpu_job)

        # 1 hour * 2 nodes * 128 cores = 256 CPU-hours
        assert result["cpu_hours"] == 256.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] > 0

    def test_gpu_production_queue(self):
        """GPU production: elapsed * numnodes * 4 / 3600."""
        job = {
            "elapsed": 3600,  # 1 hour
            "numnodes": 2,
            "numcpus": 256,
            "numgpus": 8,
            "memory": 107374182400,
            "queue": "main@desched1:gpu",
        }
        result = derecho_charge(job)

        # GPU queue uses 4 GPUs per node for production
        # 1 hour * 2 nodes * 4 = 8 GPU-hours
        assert result["gpu_hours"] == 8.0
        # CPU hours still calculated (production rate)
        assert result["cpu_hours"] == 256.0

    def test_cpu_dev_queue(self, derecho_cpu_dev_job):
        """CPU dev: elapsed * numcpus / 3600."""
        result = derecho_charge(derecho_cpu_dev_job)

        # Dev queue uses actual CPUs: 1 hour * 32 cpus = 32 CPU-hours
        assert result["cpu_hours"] == 32.0
        assert result["gpu_hours"] == 0.0

    def test_gpu_dev_queue(self):
        """GPU dev: elapsed * numgpus / 3600."""
        job = {
            "elapsed": 3600,
            "numnodes": 1,
            "numcpus": 32,
            "numgpus": 4,
            "memory": 32212254720,
            "queue": "gpudev",
        }
        result = derecho_charge(job)

        # GPU dev uses actual GPUs: 1 hour * 4 gpus = 4 GPU-hours
        assert result["gpu_hours"] == 4.0
        # CPU dev rate (actual CPUs)
        assert result["cpu_hours"] == 32.0

    def test_memory_hours(self, derecho_cpu_job):
        """Memory hours: elapsed * memory_gb / 3600."""
        result = derecho_charge(derecho_cpu_job)

        # 1 hour * 100 GB = 100 GB-hours
        assert result["memory_hours"] == pytest.approx(100.0, rel=0.01)

    def test_zero_elapsed(self):
        """Zero elapsed time should result in zero charges."""
        job = {
            "elapsed": 0,
            "numnodes": 2,
            "numcpus": 256,
            "memory": 107374182400,
            "queue": "main",
        }
        result = derecho_charge(job)

        assert result["cpu_hours"] == 0.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] == 0.0

    def test_none_values(self):
        """None values should be treated as zero."""
        job = {
            "elapsed": None,
            "numnodes": None,
            "numcpus": None,
            "memory": None,
            "queue": None,
        }
        result = derecho_charge(job)

        assert result["cpu_hours"] == 0.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] == 0.0


class TestCasperCharging:
    """Tests for Casper charging calculations."""

    def test_cpu_hours(self, casper_job):
        """CPU hours: elapsed * numcpus / 3600."""
        result = casper_charge(casper_job)

        # 1 hour * 8 cpus = 8 CPU-hours
        assert result["cpu_hours"] == 8.0

    def test_gpu_hours(self, casper_job):
        """GPU hours: elapsed * numgpus / 3600."""
        result = casper_charge(casper_job)

        # 1 hour * 2 gpus = 2 GPU-hours
        assert result["gpu_hours"] == 2.0

    def test_memory_hours(self, casper_job):
        """Memory hours: elapsed * memory_gb / 3600."""
        result = casper_charge(casper_job)

        # 1 hour * 30 GB = 30 GB-hours
        assert result["memory_hours"] == pytest.approx(30.0, rel=0.01)

    def test_zero_gpus(self):
        """Jobs without GPUs should have zero GPU-hours."""
        job = {
            "elapsed": 3600,
            "numcpus": 8,
            "numgpus": 0,
            "memory": 32212254720,
        }
        result = casper_charge(job)

        assert result["cpu_hours"] == 8.0
        assert result["gpu_hours"] == 0.0

    def test_none_values(self):
        """None values should be treated as zero."""
        job = {}
        result = casper_charge(job)

        assert result["cpu_hours"] == 0.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] == 0.0
