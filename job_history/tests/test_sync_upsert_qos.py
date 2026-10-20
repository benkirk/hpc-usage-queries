"""--upsert re-resolves a job's QoS and recomputes its charge with the new factor."""

from datetime import datetime, timezone

from sqlalchemy import text

from job_history.database import Job, JobCharge, LookupCache
from job_history.tests.test_sync_incremental import StubSyncer, _make_record

TARGET_DATE = "2025-06-01"
TARGET_DT = datetime(2025, 6, 1, 18, 0, 0, tzinfo=timezone.utc)


def _qos_and_factor(session, job_id):
    session.expire_all()
    job = session.query(Job).filter_by(job_id=job_id).one()
    charge = session.query(JobCharge).filter_by(job_id=job.id).one()
    return job.qos, charge.qos_factor


def _demote_to_regular(session, job_id):
    """Rewind a job to what pre-preempt code stored: QoS regular, factor 1.0."""
    regular_id = LookupCache(session).get_or_create_qos("regular").id
    session.execute(text(
        "UPDATE jobs SET qos_id = :q WHERE job_id = :j"), {"q": regular_id, "j": job_id})
    session.execute(text(
        "UPDATE job_charges SET qos_factor = 1.0 WHERE job_id = "
        "(SELECT id FROM jobs WHERE job_id = :j)"), {"j": job_id})
    session.commit()


class TestUpsertRefreshesQoSFactor:
    def test_preempt_queue_reclassified_on_upsert(self, in_memory_session):
        rec = {**_make_record("job.P.1", TARGET_DT, queue="pgpu"), "priority": "none"}
        StubSyncer(in_memory_session, "derecho", [rec]).sync(log_dir=None, period=TARGET_DATE)
        assert _qos_and_factor(in_memory_session, "job.P.1") == ("preempt", 0.2)

        _demote_to_regular(in_memory_session, "job.P.1")
        assert _qos_and_factor(in_memory_session, "job.P.1") == ("regular", 1.0)

        StubSyncer(in_memory_session, "derecho", [rec]).sync(
            log_dir=None, period=TARGET_DATE, upsert=True)
        assert _qos_and_factor(in_memory_session, "job.P.1") == ("preempt", 0.2)

    def test_priority_change_reprices_on_upsert(self, in_memory_session):
        rec = {**_make_record("job.E.1", TARGET_DT), "priority": "economy"}
        StubSyncer(in_memory_session, "derecho", [rec]).sync(log_dir=None, period=TARGET_DATE)
        assert _qos_and_factor(in_memory_session, "job.E.1") == ("economy", 0.7)

        StubSyncer(in_memory_session, "derecho", [{**rec, "priority": "premium"}]).sync(
            log_dir=None, period=TARGET_DATE, upsert=True)
        assert _qos_and_factor(in_memory_session, "job.E.1") == ("premium", 1.5)
