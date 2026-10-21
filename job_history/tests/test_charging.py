"""Tests for charging calculation functions."""

import types

import pytest

from job_history.sync.charging import (
    CasperCharging,
    DerechoCharging,
    QueueRule,
    SystemCharging,
    select_chunks,
)


class TestDerechoCharging:
    """Tests for Derecho charging calculations."""

    def test_cpu_production_queue(self, derecho_cpu_job):
        """CPU production: elapsed * numnodes * 128 / 3600."""
        result = DerechoCharging.calculate(derecho_cpu_job)

        # 1 hour * 2 nodes * 128 cores = 256 CPU-hours
        assert result["cpu_hours"] == 256.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] > 0

    def test_gpu_production_queue(self):
        """GPU production: elapsed * numnodes * 4 / 3600."""
        job = types.SimpleNamespace(
            elapsed=3600,  # 1 hour
            numnodes=2,
            numcpus=256,
            numgpus=8,
            memory=107374182400,
            queue="main@desched1:gpu",
        )
        result = DerechoCharging.calculate(job)

        # GPU queue uses 4 GPUs per node for production
        # 1 hour * 2 nodes * 4 = 8 GPU-hours
        assert result["gpu_hours"] == 8.0
        # GPU nodes have 64 cores: 1 hour * 2 nodes * 64 = 128 CPU-hours
        assert result["cpu_hours"] == 128.0

    def test_gpu_select_on_unnamed_queue_charges_gpu_nodes(self):
        """Node type comes from the request, not the queue name (hybrid, system, reservations)."""
        for queue in ("hybrid", "system", "R6678947"):
            job = types.SimpleNamespace(
                elapsed=3600, numnodes=4, numcpus=256, numgpus=16, memory=0,
                queue=queue, resources="4:ncpus=64:ngpus=4:ompthreads=1",
            )
            result = DerechoCharging.calculate(job)
            assert (result["cpu_hours"], result["gpu_hours"]) == (256.0, 16.0), queue

    def test_mixed_cpu_and_gpu_nodes(self):
        """A 1 GPU + 4 CPU node hybrid job: each node at its own type's rate."""
        job = types.SimpleNamespace(
            elapsed=3600, numnodes=5, numcpus=321, numgpus=1, memory=0, queue="hybrid",
            resources="1:ncpus=1:mpiprocs=1:ngpus=1:gpu_type=a100+4:ncpus=80:mpiprocs=80",
        )
        result = DerechoCharging.calculate(job)
        assert result["cpu_hours"] == 64.0 + 4 * 128.0
        assert result["gpu_hours"] == 4.0

    def test_cpu_select_charges_cpu_nodes(self, derecho_cpu_job):
        job = types.SimpleNamespace(**{**vars(derecho_cpu_job), "resources": "2:ncpus=128"})
        result = DerechoCharging.calculate(job)
        assert (result["cpu_hours"], result["gpu_hours"]) == (256.0, 0.0)

    def test_gpu_nodes_capped_at_numnodes(self):
        """Chunks packed onto fewer nodes never charge more nodes than ran."""
        job = types.SimpleNamespace(
            elapsed=3600, numnodes=1, numcpus=8, numgpus=2, memory=0, queue="gpu",
            resources="2:ncpus=4:ngpus=1",
        )
        result = DerechoCharging.calculate(job)
        assert (result["cpu_hours"], result["gpu_hours"]) == (64.0, 4.0)

    def test_cpu_dev_queue(self, derecho_cpu_dev_job):
        """CPU dev: elapsed * numcpus / 3600."""
        result = DerechoCharging.calculate(derecho_cpu_dev_job)

        # Dev queue uses actual CPUs: 1 hour * 32 cpus = 32 CPU-hours
        assert result["cpu_hours"] == 32.0
        assert result["gpu_hours"] == 0.0

    def test_gpu_dev_queue(self):
        """GPU dev: elapsed * numgpus / 3600."""
        job = types.SimpleNamespace(
            elapsed=3600,
            numnodes=1,
            numcpus=32,
            numgpus=4,
            memory=32212254720,
            queue="gpudev",
        )
        result = DerechoCharging.calculate(job)

        # GPU dev uses actual GPUs: 1 hour * 4 gpus = 4 GPU-hours
        assert result["gpu_hours"] == 4.0
        # CPU dev rate (actual CPUs)
        assert result["cpu_hours"] == 32.0

    def test_memory_hours(self, derecho_cpu_job):
        """Memory hours: elapsed * memory_gb / 3600."""
        result = DerechoCharging.calculate(derecho_cpu_job)

        # 1 hour * 100 GB = 100 GB-hours
        assert result["memory_hours"] == pytest.approx(100.0, rel=0.01)

    def test_zero_elapsed(self):
        """Zero elapsed time should result in zero charges."""
        job = types.SimpleNamespace(
            elapsed=0,
            numnodes=2,
            numcpus=256,
            memory=107374182400,
            queue="main",
        )
        result = DerechoCharging.calculate(job)

        assert result["cpu_hours"] == 0.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] == 0.0

    def test_none_values(self):
        """None values should be treated as zero."""
        job = types.SimpleNamespace(
            elapsed=None,
            numnodes=None,
            numcpus=None,
            memory=None,
            queue=None,
        )
        result = DerechoCharging.calculate(job)

        assert result["cpu_hours"] == 0.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] == 0.0

    def test_jhublogin_queue(self):
        """jhublogin queue: hours tracked but qos_factor=0.0 so effective charge is zero."""
        job = types.SimpleNamespace(
            elapsed=3600,  # 1 hour
            numnodes=1,
            numcpus=128,
            numgpus=0,
            memory=107374182400,  # 100 GB
            queue="jhublogin",
            priority="regular",
        )
        result = DerechoCharging.calculate(job)

        # Resource hours are tracked (non-zero)
        assert result["cpu_hours"] > 0
        assert result["memory_hours"] > 0
        # qos_factor is 0.0 — effective charge is zero
        assert result["qos_factor"] == 0.0
        assert result["cpu_hours"] * result["qos_factor"] == 0.0
        assert result["memory_hours"] * result["qos_factor"] == 0.0


class TestSelectChunks:
    @pytest.mark.parametrize("select, expected", [
        ("1:ncpus=64:mem=487GB:mpiprocs=4:ngpus=4", [(1, 4)]),
        ("2:ncpus=128", [(2, 0)]),
        ("ncpus=8:ngpus=1", [(1, 1)]),
        ("1:ncpus=128+1:ncpus=64:ngpus=4:ompthreads=1", [(1, 0), (1, 4)]),
        ("", []),
        (None, []),
    ])
    def test_chunks(self, select, expected):
        assert select_chunks(select) == expected


class TestQueueRules:
    def test_unlisted_queue_is_exclusive_with_priority_qos(self):
        assert DerechoCharging.queue_rule("cpu") == QueueRule()
        assert DerechoCharging.queue_rule(None) == QueueRule()

    @pytest.mark.parametrize("queue", ["cpudev", "gpudev", "GPUDEV"])
    def test_derecho_dev_queues_are_shared(self, queue):
        assert DerechoCharging.queue_rule(queue).shared

    def test_qos_overrides(self):
        assert SystemCharging.qos_overrides() == {"jhublogin": "uncharged"}
        assert CasperCharging.qos_overrides() == {"jhublogin": "uncharged"}
        assert DerechoCharging.qos_overrides() == {
            "jhublogin": "uncharged", "pcpu": "preempt", "pgpu": "preempt",
        }

    def test_priority_overrides(self):
        """The backfill's priority CASE: every priority that is not the regular default."""
        assert SystemCharging.priority_overrides() == {
            "premium": "premium", "economy": "economy", "special": "special",
        }


class TestCasperCharging:
    """Tests for Casper charging calculations."""

    def test_cpu_hours(self, casper_job):
        """CPU hours: elapsed * numcpus / 3600."""
        result = CasperCharging.calculate(casper_job)

        # 1 hour * 8 cpus = 8 CPU-hours
        assert result["cpu_hours"] == 8.0

    def test_gpu_hours(self, casper_job):
        """GPU hours: elapsed * numgpus / 3600."""
        result = CasperCharging.calculate(casper_job)

        # 1 hour * 2 gpus = 2 GPU-hours
        assert result["gpu_hours"] == 2.0

    def test_memory_hours(self, casper_job):
        """Memory hours: elapsed * memory_gb / 3600."""
        result = CasperCharging.calculate(casper_job)

        # 1 hour * 30 GB = 30 GB-hours
        assert result["memory_hours"] == pytest.approx(30.0, rel=0.01)

    def test_zero_gpus(self):
        """Jobs without GPUs should have zero GPU-hours."""
        job = types.SimpleNamespace(
            elapsed=3600,
            numcpus=8,
            numgpus=0,
            memory=32212254720,
        )
        result = CasperCharging.calculate(job)

        assert result["cpu_hours"] == 8.0
        assert result["gpu_hours"] == 0.0

    def test_none_values(self):
        """None values should be treated as zero."""
        job = types.SimpleNamespace()
        result = CasperCharging.calculate(job)

        assert result["cpu_hours"] == 0.0
        assert result["gpu_hours"] == 0.0
        assert result["memory_hours"] == 0.0

    def test_jhublogin_queue(self):
        """jhublogin queue: hours tracked but qos_factor=0.0 so effective charge is zero."""
        job = types.SimpleNamespace(
            elapsed=3600,  # 1 hour
            numcpus=8,
            numgpus=0,
            memory=32212254720,  # 30 GB
            queue="jhublogin",
            priority="regular",
        )
        result = CasperCharging.calculate(job)

        # Resource hours are tracked (non-zero)
        assert result["cpu_hours"] > 0
        assert result["memory_hours"] > 0
        # qos_factor is 0.0 — effective charge is zero
        assert result["qos_factor"] == 0.0
        assert result["cpu_hours"] * result["qos_factor"] == 0.0
        assert result["memory_hours"] * result["qos_factor"] == 0.0
