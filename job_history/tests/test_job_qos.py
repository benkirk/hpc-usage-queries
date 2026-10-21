"""Tests for the normalized JobQoS lookup table.

Covers schema, seed rows, hybrid property roundtrip, before_flush FK
resolution, LookupCache.get_or_create_qos, _resolve_qos_name precedence,
and the bin/update_jobs_db.py backfill against an in-memory SQLite mirror.
"""

import types
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from job_history.database import (
    Base,
    Job,
    JobCharge,
    JobQoS,
    LookupCache,
    Queue,
)
from job_history.database.session import (
    JOB_QOS_SEED,
    _ensure_db_triggers,
    _ensure_qos_seed_rows,
    _rename_jhublogin_to_uncharged,
)
from job_history.sync.charging import (
    CasperCharging,
    DerechoCharging,
    SystemCharging,
)


@pytest.fixture
def seeded_engine():
    """In-memory SQLite engine with schema + canonical JobQoS seed rows."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    _ensure_db_triggers(engine)
    _ensure_qos_seed_rows(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def seeded_session(seeded_engine):
    Session = sessionmaker(bind=seeded_engine)
    session = Session()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Seed table
# ---------------------------------------------------------------------------

class TestSeedRows:
    def test_seed_rows_present(self, seeded_session):
        rows = seeded_session.query(JobQoS).order_by(JobQoS.name).all()
        names = sorted(r.name for r in rows)
        assert names == ["economy", "preempt", "premium", "regular", "special", "uncharged"]
        assert len(rows) == 6

    def test_seed_factors_match_constants(self, seeded_session):
        by_name = {r.name: r for r in seeded_session.query(JobQoS).all()}
        expected = {name: factor for name, factor, _ in JOB_QOS_SEED}
        for name, factor in expected.items():
            assert by_name[name].factor == factor
            assert by_name[name].active is True

    def test_seed_is_idempotent(self, seeded_engine, seeded_session):
        _ensure_qos_seed_rows(seeded_engine)
        _ensure_qos_seed_rows(seeded_engine)
        assert seeded_session.query(JobQoS).count() == 6


# ---------------------------------------------------------------------------
# Hybrid property roundtrip via before_flush listener
# ---------------------------------------------------------------------------

class TestQoSHybrid:
    def test_setter_stashes_pending(self, seeded_session):
        job = Job(
            job_id="1.test",
            submit=datetime(2026, 5, 1, tzinfo=timezone.utc),
            qos="premium",
        )
        # Before flush: FK not yet resolved
        assert job._pending_qos_name == "premium"
        assert job.qos_id is None
        assert job.qos_obj is None

    def test_roundtrip_premium(self, seeded_session):
        job = Job(
            job_id="42.test",
            submit=datetime(2026, 5, 1, tzinfo=timezone.utc),
            qos="premium",
        )
        seeded_session.add(job)
        seeded_session.flush()

        assert job.qos == "premium"
        assert job.qos_obj is not None
        assert job.qos_obj.factor == 1.5
        assert job.qos_id is not None

    def test_roundtrip_uncharged(self, seeded_session):
        job = Job(
            job_id="43.test",
            submit=datetime(2026, 5, 1, tzinfo=timezone.utc),
            qos="uncharged",
        )
        seeded_session.add(job)
        seeded_session.flush()

        assert job.qos == "uncharged"
        assert job.qos_obj.factor == 0.0

    def test_roundtrip_special(self, seeded_session):
        job = Job(
            job_id="46.test",
            submit=datetime(2026, 5, 1, tzinfo=timezone.utc),
            qos="special",
        )
        seeded_session.add(job)
        seeded_session.flush()

        assert job.qos == "special"
        assert job.qos_obj.factor == 1.0

    def test_setter_none_clears_fk(self, seeded_session):
        job = Job(
            job_id="44.test",
            submit=datetime(2026, 5, 1, tzinfo=timezone.utc),
            qos="economy",
        )
        seeded_session.add(job)
        seeded_session.flush()
        assert job.qos == "economy"

        job.qos = None
        seeded_session.flush()
        assert job.qos_id is None
        assert job.qos_obj is None

    def test_to_dict_includes_qos(self, seeded_session):
        job = Job(
            job_id="45.test",
            submit=datetime(2026, 5, 1, tzinfo=timezone.utc),
            qos="regular",
        )
        seeded_session.add(job)
        seeded_session.flush()
        d = job.to_dict()
        assert "qos" in d
        assert d["qos"] == "regular"


# ---------------------------------------------------------------------------
# LookupCache.get_or_create_qos
# ---------------------------------------------------------------------------

class TestLookupCacheQoS:
    def test_returns_seeded_row_without_insert(self, seeded_session):
        cache = LookupCache(seeded_session, auto_flush=False)
        obj = cache.get_or_create_qos("premium")
        assert obj.factor == 1.5
        # No duplicate row inserted
        assert seeded_session.query(JobQoS).filter_by(name="premium").count() == 1

    def test_creates_new_row_for_unknown_name(self, seeded_session):
        cache = LookupCache(seeded_session)
        obj = cache.get_or_create_qos("custom_tier", factor=2.0)
        seeded_session.flush()
        assert obj.factor == 2.0
        assert seeded_session.query(JobQoS).filter_by(name="custom_tier").one().factor == 2.0

    def test_unseeded_db_creates_seed_name_at_seed_factor(self):
        """Sync can meet a new seed name (preempt) before init_db seeds it."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        try:
            cache = LookupCache(session)
            assert cache.get_or_create_qos("preempt").factor == 0.2
            assert cache.get_or_create_qos("not_a_seed").factor == 1.0
            session.commit()
            _ensure_qos_seed_rows(engine)
            assert session.query(JobQoS).filter_by(name="preempt").one().factor == 0.2
        finally:
            session.close()
            engine.dispose()

    def test_dedupes_within_session(self, seeded_session):
        cache = LookupCache(seeded_session)
        a = cache.get_or_create_qos("premium")
        b = cache.get_or_create_qos("premium")
        assert a is b


# ---------------------------------------------------------------------------
# _resolve_qos_name precedence
# ---------------------------------------------------------------------------

class TestResolveQoSName:
    def test_jhublogin_queue_overrides_priority(self):
        job = types.SimpleNamespace(queue="jhublogin", priority="regular")
        assert SystemCharging._resolve_qos_name(job) == "uncharged"

    def test_jhublogin_queue_overrides_premium(self):
        job = types.SimpleNamespace(queue="jhublogin", priority="premium")
        assert SystemCharging._resolve_qos_name(job) == "uncharged"

    def test_premium_priority(self):
        job = types.SimpleNamespace(queue="main", priority="premium")
        assert SystemCharging._resolve_qos_name(job) == "premium"

    def test_economy_priority(self):
        job = types.SimpleNamespace(queue="main", priority="economy")
        assert SystemCharging._resolve_qos_name(job) == "economy"

    def test_special_priority(self):
        job = types.SimpleNamespace(queue="main", priority="special")
        assert SystemCharging._resolve_qos_name(job) == "special"

    def test_unknown_priority_falls_back_to_regular(self):
        job = types.SimpleNamespace(queue="main", priority="bogus")
        assert SystemCharging._resolve_qos_name(job) == "regular"

    def test_none_priority_is_regular(self):
        job = types.SimpleNamespace(queue="main", priority=None)
        assert SystemCharging._resolve_qos_name(job) == "regular"

    def test_accepts_dict_input(self):
        rec = {"queue": "jhublogin", "priority": "regular"}
        assert SystemCharging._resolve_qos_name(rec) == "uncharged"

    def test_dict_with_premium(self):
        rec = {"queue": "main", "priority": "premium"}
        assert SystemCharging._resolve_qos_name(rec) == "premium"


# ---------------------------------------------------------------------------
# Per-machine queue rules: Derecho preempt queues
# ---------------------------------------------------------------------------

class TestPreemptQueues:
    @pytest.mark.parametrize("queue", ["pcpu", "pgpu", "PGPU"])
    @pytest.mark.parametrize("priority", ["none", "", None, "regular", "premium", "economy"])
    def test_derecho_preempt_queue_overrides_priority(self, queue, priority):
        rec = {"queue": queue, "priority": priority}
        assert DerechoCharging._resolve_qos_name(rec) == "preempt"
        assert SystemCharging.resolve_qos_name("derecho", rec) == "preempt"

    @pytest.mark.parametrize("queue", ["pcpu", "pgpu"])
    def test_casper_has_no_preempt_queues(self, queue):
        rec = {"queue": queue, "priority": "premium"}
        assert SystemCharging.resolve_qos_name("casper", rec) == "premium"

    def test_base_map_has_no_preempt_queues(self):
        rec = {"queue": "pgpu", "priority": "none"}
        assert SystemCharging._resolve_qos_name(rec) == "regular"

    @pytest.mark.parametrize("machine", ["derecho", "casper"])
    def test_jhublogin_kept_on_every_machine(self, machine):
        rec = {"queue": "jhublogin", "priority": "premium"}
        assert SystemCharging.resolve_qos_name(machine, rec) == "uncharged"

    def test_unknown_machine_raises(self):
        with pytest.raises(ValueError, match="Unknown machine"):
            SystemCharging.resolve_qos_name("cheyenne", {"queue": "pgpu"})

    def test_derecho_pgpu_charges_gpus_at_preempt_factor(self):
        job = types.SimpleNamespace(
            elapsed=3600, numnodes=2, numcpus=128, numgpus=8,
            memory=0, queue="pgpu", priority="none",
        )
        result = DerechoCharging.calculate(job)
        assert result["qos_factor"] == 0.2
        assert result["gpu_hours"] == 8.0
        assert result["cpu_hours"] == 128.0   # 64-core GPU nodes

    def test_derecho_pcpu_charges_cpus_at_preempt_factor(self):
        job = types.SimpleNamespace(
            elapsed=3600, numnodes=1, numcpus=128, numgpus=0,
            memory=0, queue="pcpu", priority="premium",
        )
        result = DerechoCharging.calculate(job)
        assert result["qos_factor"] == 0.2
        assert result["cpu_hours"] == 128.0
        assert result["gpu_hours"] == 0

    def test_casper_pgpu_keeps_priority_factor(self):
        job = types.SimpleNamespace(
            elapsed=3600, numcpus=1, numgpus=1, memory=0,
            queue="pgpu", priority="economy",
        )
        assert CasperCharging.calculate(job)["qos_factor"] == 0.7


# ---------------------------------------------------------------------------

class TestGetQoSFactorFallback:
    def test_fallback_when_qos_obj_missing(self):
        # SimpleNamespace simulates legacy/test-fixture jobs with no FK row.
        job = types.SimpleNamespace(queue="main", priority="premium")
        assert SystemCharging._get_qos_factor(job) == 1.5

    def test_fallback_jhublogin_queue_is_uncharged(self):
        job = types.SimpleNamespace(queue="jhublogin", priority="regular")
        assert SystemCharging._get_qos_factor(job) == 0.0

    def test_fallback_special(self):
        job = types.SimpleNamespace(queue="main", priority="special")
        assert SystemCharging._get_qos_factor(job) == 1.0

    def test_prefers_qos_obj_factor(self):
        # A SimpleNamespace whose qos_obj overrides the priority/queue mapping
        qos_stub = types.SimpleNamespace(factor=0.42)
        job = types.SimpleNamespace(
            queue="main", priority="premium", qos_obj=qos_stub,
        )
        # 0.42 from the FK row, NOT 1.5 from the fallback.
        assert SystemCharging._get_qos_factor(job) == 0.42

    def test_charging_calculate_consistency(self):
        """DerechoCharging.calculate still emits the right qos_factor."""
        job = types.SimpleNamespace(
            elapsed=3600, numnodes=1, numcpus=128, numgpus=0,
            memory=107374182400, queue="main", priority="economy",
        )
        result = DerechoCharging.calculate(job)
        assert result["qos_factor"] == 0.7


# ---------------------------------------------------------------------------
# Backfill SQL (bin/update_jobs_db.py)
# ---------------------------------------------------------------------------

class TestBackfill:
    def test_backfill_maps_priority_and_queue(self, seeded_engine, seeded_session):
        """Insert assorted jobs without qos_id, run the backfill, verify mapping."""
        # Set up queues
        main_q = Queue(queue_name="main")
        jhub_q = Queue(queue_name="jhublogin")
        seeded_session.add_all([main_q, jhub_q])
        seeded_session.flush()

        # Insert jobs directly via Core API to bypass the hybrid setter (so
        # qos_id starts NULL, exactly like a pre-migration row).
        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, priority, queue_id) VALUES "
            "('a', :s, 'premium', :mid), "
            "('b', :s, 'economy', :mid), "
            "('c', :s, 'regular', :mid), "
            "('d', :s, NULL, :mid), "
            "('e', :s, 'premium', :jid)"   # jhublogin overrides premium
        ), {
            "s": datetime(2026, 5, 1).isoformat(),
            "mid": main_q.id,
            "jid": jhub_q.id,
        })
        seeded_session.commit()

        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine)

        # Verify mapping
        by_jobid = dict(seeded_session.execute(text(
            "SELECT j.job_id, q.name FROM jobs j JOIN job_qos q ON q.id = j.qos_id"
        )).all())
        assert by_jobid["a"] == "premium"
        assert by_jobid["b"] == "economy"
        assert by_jobid["c"] == "regular"
        assert by_jobid["d"] == "regular"      # NULL priority → regular
        assert by_jobid["e"] == "uncharged"    # jhublogin queue → uncharged

    @staticmethod
    def _insert_preempt_jobs(session):
        pcpu_q = Queue(queue_name="pcpu")
        pgpu_q = Queue(queue_name="pgpu")
        session.add_all([pcpu_q, pgpu_q])
        session.flush()
        session.execute(text(
            "INSERT INTO jobs (job_id, submit, priority, queue_id) VALUES "
            "('c1', :s, 'none',    :cq), "
            "('c2', :s, 'premium', :cq), "
            "('g1', :s, NULL,      :gq), "
            "('g2', :s, 'economy', :gq)"
        ), {"s": datetime(2026, 5, 1).isoformat(), "cq": pcpu_q.id, "gq": pgpu_q.id})
        session.commit()

    def _qos_by_jobid(self, session):
        return dict(session.execute(text(
            "SELECT j.job_id, q.name FROM jobs j JOIN job_qos q ON q.id = j.qos_id"
        )).all())

    def test_backfill_derecho_preempt_queues(self, seeded_engine, seeded_session):
        self._insert_preempt_jobs(seeded_session)
        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine, "derecho")
        assert self._qos_by_jobid(seeded_session) == {
            "c1": "preempt", "c2": "preempt", "g1": "preempt", "g2": "preempt",
        }

    def test_backfill_casper_leaves_preempt_queue_names_alone(self, seeded_engine, seeded_session):
        self._insert_preempt_jobs(seeded_session)
        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine, "casper")
        assert self._qos_by_jobid(seeded_session) == {
            "c1": "regular", "c2": "premium", "g1": "regular", "g2": "economy",
        }

    def test_backfill_reclassifies_existing_regular_rows(self, seeded_engine, seeded_session):
        """Rows already backfilled as regular move to preempt on a re-run."""
        self._insert_preempt_jobs(seeded_session)
        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine)
        assert self._qos_by_jobid(seeded_session)["c1"] == "regular"
        backfill_qos_id(seeded_engine, "derecho", chunk_size=1)
        assert set(self._qos_by_jobid(seeded_session).values()) == {"preempt"}

    def test_backfill_requires_seeded_target_qos(self, seeded_engine, seeded_session):
        seeded_session.execute(text("DELETE FROM job_qos WHERE name = 'preempt'"))
        seeded_session.commit()
        from bin.update_jobs_db import backfill_qos_id
        with pytest.raises(RuntimeError, match="preempt"):
            backfill_qos_id(seeded_engine, "derecho")

    def test_backfill_handles_special_priority(self, seeded_engine, seeded_session):
        main_q = Queue(queue_name="main")
        seeded_session.add(main_q)
        seeded_session.flush()
        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, priority, queue_id) "
            "VALUES ('s', :s, 'special', :mid)"
        ), {"s": datetime(2026, 5, 1).isoformat(), "mid": main_q.id})
        seeded_session.commit()

        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine)
        name = seeded_session.execute(text(
            "SELECT q.name FROM jobs j JOIN job_qos q ON q.id = j.qos_id WHERE j.job_id = 's'"
        )).scalar()
        assert name == "special"

    def test_backfill_chunked_loop(self, seeded_engine, seeded_session):
        """With chunk_size smaller than the row count, every row is still
        correctly populated and the loop emits one transaction per chunk."""
        main_q = Queue(queue_name="main")
        jhub_q = Queue(queue_name="jhublogin")
        seeded_session.add_all([main_q, jhub_q])
        seeded_session.flush()

        # Insert 7 rows so chunk_size=2 → 4 chunks (ids 1..7).
        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, priority, queue_id) VALUES "
            "('a', :s, 'premium', :mid), "
            "('b', :s, 'economy', :mid), "
            "('c', :s, 'regular', :mid), "
            "('d', :s, 'special', :mid), "
            "('e', :s, NULL,      :mid), "
            "('f', :s, 'premium', :jid), "
            "('g', :s, 'regular', :jid)"
        ), {
            "s": datetime(2026, 5, 1).isoformat(),
            "mid": main_q.id,
            "jid": jhub_q.id,
        })
        seeded_session.commit()

        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine, chunk_size=2)

        by_jobid = dict(seeded_session.execute(text(
            "SELECT j.job_id, q.name FROM jobs j JOIN job_qos q ON q.id = j.qos_id"
        )).all())
        assert by_jobid == {
            "a": "premium", "b": "economy", "c": "regular",
            "d": "special", "e": "regular",
            "f": "uncharged", "g": "uncharged",   # jhublogin queue wins
        }

    def test_backfill_resumes_after_partial_run(self, seeded_engine, seeded_session):
        """Simulate an interrupted run by pre-populating qos_id on half the
        rows.  The chunked backfill should leave those rows unchanged (the
        DISTINCT FROM guard filters them out) and complete the rest."""
        main_q = Queue(queue_name="main")
        seeded_session.add(main_q)
        seeded_session.flush()

        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, priority, queue_id) VALUES "
            "('p', :s, 'premium', :mid), "
            "('e', :s, 'economy', :mid), "
            "('r', :s, 'regular', :mid)"
        ), {"s": datetime(2026, 5, 1).isoformat(), "mid": main_q.id})
        seeded_session.commit()

        # Pre-populate qos_id on row 'e' to simulate a partial prior run.
        econ_id = seeded_session.execute(text(
            "SELECT id FROM job_qos WHERE name = 'economy'"
        )).scalar()
        seeded_session.execute(text(
            "UPDATE jobs SET qos_id = :qid WHERE job_id = 'e'"
        ), {"qid": econ_id})
        seeded_session.commit()

        from bin.update_jobs_db import backfill_qos_id
        backfill_qos_id(seeded_engine, chunk_size=1)

        by_jobid = dict(seeded_session.execute(text(
            "SELECT j.job_id, q.name FROM jobs j JOIN job_qos q ON q.id = j.qos_id"
        )).all())
        assert by_jobid == {"p": "premium", "e": "economy", "r": "regular"}

    def test_backfill_is_idempotent(self, seeded_engine, seeded_session):
        main_q = Queue(queue_name="main")
        seeded_session.add(main_q)
        seeded_session.flush()

        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, priority, queue_id) "
            "VALUES ('x', :s, 'economy', :mid)"
        ), {"s": datetime(2026, 5, 1).isoformat(), "mid": main_q.id})
        seeded_session.commit()

        from bin.update_jobs_db import backfill_qos_id
        # First run assigns qos_id
        backfill_qos_id(seeded_engine)
        first = seeded_session.execute(text(
            "SELECT qos_id FROM jobs WHERE job_id = 'x'"
        )).scalar()
        # Second run is a no-op
        backfill_qos_id(seeded_engine)
        second = seeded_session.execute(text(
            "SELECT qos_id FROM jobs WHERE job_id = 'x'"
        )).scalar()
        assert first == second


# ---------------------------------------------------------------------------
# _rename_jhublogin_to_uncharged migration helper
# ---------------------------------------------------------------------------

class TestRenameJhubloginToUncharged:
    def test_renames_legacy_row_preserving_fk(self, seeded_engine, seeded_session):
        """A pre-existing 'jhublogin' row gets renamed in place; jobs.qos_id
        references continue to resolve to the (now-renamed) row."""
        # Wipe the seeded 'uncharged' row to simulate a pre-rename state, then
        # insert a legacy 'jhublogin' row in its place.
        seeded_session.execute(text("DELETE FROM job_qos WHERE name = 'uncharged'"))
        seeded_session.execute(text(
            "INSERT INTO job_qos (name, factor, active) VALUES ('jhublogin', 0.0, 1)"
        ))
        seeded_session.commit()

        jhub_id = seeded_session.execute(text(
            "SELECT id FROM job_qos WHERE name = 'jhublogin'"
        )).scalar()

        # Insert a Job whose qos_id references the legacy row.
        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, qos_id) VALUES ('legacy', :s, :qid)"
        ), {"s": datetime(2026, 5, 1).isoformat(), "qid": jhub_id})
        seeded_session.commit()

        _rename_jhublogin_to_uncharged(seeded_engine)

        # Row is renamed in place — same id, new name.
        renamed = seeded_session.execute(text(
            "SELECT id, name FROM job_qos WHERE id = :qid"
        ), {"qid": jhub_id}).one()
        assert renamed.name == "uncharged"
        # Job's qos_id is untouched and still resolves correctly.
        job_qos_id = seeded_session.execute(text(
            "SELECT qos_id FROM jobs WHERE job_id = 'legacy'"
        )).scalar()
        assert job_qos_id == jhub_id
        # And the hybrid resolves to 'uncharged' now.
        seeded_session.expire_all()
        job = seeded_session.query(Job).filter_by(job_id="legacy").one()
        assert job.qos == "uncharged"

    def test_noop_when_jhublogin_absent(self, seeded_engine, seeded_session):
        """Already-seeded DBs (no legacy jhublogin row) are left untouched."""
        before = seeded_session.execute(text(
            "SELECT id, name FROM job_qos ORDER BY id"
        )).all()
        _rename_jhublogin_to_uncharged(seeded_engine)
        after = seeded_session.execute(text(
            "SELECT id, name FROM job_qos ORDER BY id"
        )).all()
        assert before == after

    def test_repoints_fk_when_both_rows_exist(self, seeded_engine, seeded_session):
        """Defensive path: if both 'jhublogin' and 'uncharged' exist, FKs are
        re-pointed at 'uncharged' and the legacy row is deleted."""
        # 'uncharged' is already seeded; insert a stray 'jhublogin' row too.
        seeded_session.execute(text(
            "INSERT INTO job_qos (name, factor, active) VALUES ('jhublogin', 0.0, 1)"
        ))
        seeded_session.commit()
        jhub_id = seeded_session.execute(text(
            "SELECT id FROM job_qos WHERE name = 'jhublogin'"
        )).scalar()
        unc_id = seeded_session.execute(text(
            "SELECT id FROM job_qos WHERE name = 'uncharged'"
        )).scalar()

        seeded_session.execute(text(
            "INSERT INTO jobs (job_id, submit, qos_id) VALUES ('dup', :s, :qid)"
        ), {"s": datetime(2026, 5, 1).isoformat(), "qid": jhub_id})
        seeded_session.commit()

        _rename_jhublogin_to_uncharged(seeded_engine)

        # Legacy row deleted, FK re-pointed.
        assert seeded_session.execute(text(
            "SELECT COUNT(*) FROM job_qos WHERE name = 'jhublogin'"
        )).scalar() == 0
        new_qos_id = seeded_session.execute(text(
            "SELECT qos_id FROM jobs WHERE job_id = 'dup'"
        )).scalar()
        assert new_qos_id == unc_id

    def test_idempotent(self, seeded_engine):
        """Running the rename twice is a no-op on the second invocation."""
        _rename_jhublogin_to_uncharged(seeded_engine)
        _rename_jhublogin_to_uncharged(seeded_engine)


# ---------------------------------------------------------------------------
# JobQueries.list_qos_names
# ---------------------------------------------------------------------------

class TestListQoSNames:
    """``JobQueries.list_qos_names`` lets a consumer (e.g. SAM webapp) populate
    a QoS filter dropdown without hardcoding the seed list."""

    def test_returns_seeded_names_alphabetical(self, seeded_session):
        from job_history.queries.jobs import JobQueries
        names = JobQueries(seeded_session).list_qos_names()
        assert names == ["economy", "preempt", "premium", "regular", "special", "uncharged"]

    def test_excludes_inactive_by_default(self, seeded_session):
        from job_history.queries.jobs import JobQueries
        seeded_session.execute(text(
            "UPDATE job_qos SET active = 0 WHERE name = 'economy'"
        ))
        seeded_session.commit()
        names = JobQueries(seeded_session).list_qos_names()
        assert "economy" not in names
        assert names == ["preempt", "premium", "regular", "special", "uncharged"]

    def test_active_only_false_includes_inactive(self, seeded_session):
        from job_history.queries.jobs import JobQueries
        seeded_session.execute(text(
            "UPDATE job_qos SET active = 0 WHERE name = 'economy'"
        ))
        seeded_session.commit()
        names = JobQueries(seeded_session).list_qos_names(active_only=False)
        assert "economy" in names
        assert len(names) == 6
