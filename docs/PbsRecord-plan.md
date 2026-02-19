# Implementation Plan: JobRecords ORM for Compressed PbsRecord Storage

## Context

Currently, when syncing jobs from local PBS accounting logs, we parse raw PBS records into `PbsRecord` objects (via the `pbsparse` library), extract their fields into a normalized database dictionary, and then **discard** the original `PbsRecord` objects. The parsed dictionary includes a `record_object` key containing the full `PbsRecord`, but this gets silently dropped during `bulk_insert_mappings()` because the `Job` model has no column for it.

**Problem**: We lose access to the raw PBS record data, which contains additional context and metadata that might be useful for debugging, validation, or future analysis.

**Solution**: Create an optional `JobRecords` table to store gzip-compressed, pickled `PbsRecord` objects with a 1-to-1 relationship to the `Job` model. This enables rapid reconstruction of the full PBS record from any job query.

**Scope**: This feature applies **only** to jobs synced from local PBS logs (via `sync_pbs_logs_bulk`). Jobs synced via SSH (via `sync_ssh_jobs_bulk`) use the `qhist` command and don't have access to raw `PbsRecord` objects, so they won't have `JobRecords`.

## Design Decisions

Based on user preferences:
- **Population**: Automatic during PBS log sync (zero extra configuration needed)
- **Compression**: gzip (better compression ratio ~15-20% vs lz4's ~18-25%)
- **API**: `job.pbs_record` property for convenient access
- **Storage**: ~400-800 bytes per job compressed (~60-80% reduction from 2-4 KB uncompressed)

## Implementation Plan

### 1. Add JobRecord Model

**File**: `job_history/models.py`

Add new ORM model following the `JobCharge` pattern (lines 217-237):

```python
class JobRecord(Base):
    """Compressed, pickled PbsRecord storage.

    Stores the raw PBS accounting record object for jobs imported from local
    PBS logs. Not available for jobs synced via SSH (qhist command).
    """

    __tablename__ = "job_records"

    # Primary key = foreign key (true 1-to-1, matches JobCharge pattern)
    job_id = Column(Integer, primary_key=True)

    # Gzip-compressed pickle of PbsRecord object
    compressed_data = Column(LargeBinary, nullable=False)

    # Metadata for debugging/auditing
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        ForeignKeyConstraint(['job_id'], ['jobs.id'], ondelete='CASCADE'),
    )

    def __repr__(self):
        return f"<JobRecord(job_id={self.job_id})>"
```

**Key design choices**:
- `job_id` as PK (not separate auto-increment): Enforces true 1-to-1 relationship
- `LargeBinary` column type: Maps to SQLite BLOB for binary data
- CASCADE delete: When Job deleted, JobRecord auto-deletes (referential integrity)
- `created_at`: Useful for tracking when record was stored

### 2. Add `pbs_record` Property to Job

**File**: `job_history/models.py`

Add property to `Job` class (after `calculate_charges` method around line 215):

```python
@property
def pbs_record(self):
    """Retrieve and unpickle the original PbsRecord object.

    Returns None if no JobRecord exists (e.g., job was synced via SSH).
    Uses lazy loading with instance-level cache to avoid repeated decompression.

    Returns:
        PbsRecord object or None
    """
    # Check instance cache first (avoid re-decompressing on multiple accesses)
    if hasattr(self, '_cached_pbs_record'):
        return self._cached_pbs_record

    # Query for JobRecord (will be None for SSH-synced jobs)
    from sqlalchemy.orm import object_session
    session = object_session(self)
    if session is None:
        self._cached_pbs_record = None
        return None

    job_record = session.query(JobRecord).filter_by(job_id=self.id).first()

    if job_record is None:
        self._cached_pbs_record = None
        return None

    # Decompress and unpickle
    import gzip
    import pickle
    try:
        decompressed = gzip.decompress(job_record.compressed_data)
        pbs_record_obj = pickle.loads(decompressed)
        self._cached_pbs_record = pbs_record_obj
        return pbs_record_obj
    except Exception as e:
        # Log error but don't crash (handle data corruption gracefully)
        from .log_config import get_logger
        logger = get_logger(__name__)
        logger.error(f"Failed to decompress/unpickle JobRecord for job {self.id}: {e}")
        self._cached_pbs_record = None
        return None
```

**Caching strategy**:
- Instance-level cache (`_cached_pbs_record`): Avoids decompression on repeated access within same Job instance
- Lazy loading: Only queries when property accessed
- Graceful degradation: Returns None for missing/corrupted records

**Why `@property` not `@hybrid_property`**: We never query by PbsRecord contents (no SQL expression needed), so simpler property is sufficient.

### 3. Populate JobRecords During Sync

**File**: `job_history/sync.py`

Modify `_insert_batch()` function to insert JobRecords after JobCharge insertion (after line 547):

```python
# Calculate charges for newly inserted jobs
if importer and rows_inserted > 0:
    # ... existing JobCharge logic (lines 516-547) ...

    # NEW: Insert JobRecords for PBS log imports (if record_object present)
    job_ids_map = {(r['job_id'], r['submit']): r for r in prepared if 'record_object' in r}

    if job_ids_map:
        from .models import JobRecord
        import pickle
        import gzip

        job_record_data = []
        for job in jobs:
            # Match job to its original record by (job_id, submit)
            submit_naive = job.submit.replace(tzinfo=None) if job.submit and job.submit.tzinfo else job.submit
            record = job_ids_map.get((job.job_id, submit_naive))

            if record and 'record_object' in record:
                pbs_record = record['record_object']

                # Compress and pickle
                pickled = pickle.dumps(pbs_record, protocol=pickle.HIGHEST_PROTOCOL)
                compressed = gzip.compress(pickled, compresslevel=6)  # Balance speed vs size

                job_record_data.append({
                    'job_id': job.id,
                    'compressed_data': compressed,
                })

        if job_record_data:
            session.bulk_insert_mappings(JobRecord, job_record_data)
```

**Integration details**:
- Runs in same transaction as Job + JobCharge inserts (atomicity)
- Only processes records with `record_object` key (PBS log imports)
- Silently skips SSH imports (no `record_object` in those dicts)
- Uses `compresslevel=6`: Good balance between speed and compression
- Matches jobs to records by (job_id, submit) tuple

### 4. Export JobRecord Model

**File**: `job_history/__init__.py`

Add JobRecord to exports (around line 14, following the JobCharge pattern):

```python
from .models import Account, DailySummary, Job, JobCharge, JobRecord, Queue, User
```

### 5. Add Unit Tests

**File**: `tests/test_models.py`

Add tests for JobRecord functionality:

```python
def test_job_record_round_trip(session_derecho):
    """Test pickle → compress → store → retrieve → decompress → unpickle."""
    from job_history.models import Job, JobRecord
    import pickle
    import gzip

    # Create a mock PbsRecord-like object
    class MockPbsRecord:
        def __init__(self):
            self.id = "123456.desched1"
            self.user = "testuser"
            self.queue = "cpu"

    pbs_record = MockPbsRecord()

    # Create Job
    job = Job(job_id="123456.desched1", submit=datetime.utcnow())
    job.user = "testuser"
    session_derecho.add(job)
    session_derecho.flush()

    # Create JobRecord
    compressed = gzip.compress(pickle.dumps(pbs_record))
    job_record = JobRecord(job_id=job.id, compressed_data=compressed)
    session_derecho.add(job_record)
    session_derecho.commit()

    # Retrieve via property
    retrieved_record = job.pbs_record
    assert retrieved_record is not None
    assert retrieved_record.id == "123456.desched1"
    assert retrieved_record.user == "testuser"

def test_job_without_record(session_derecho):
    """Jobs without JobRecord should return None."""
    job = Job(job_id="ssh.job", submit=datetime.utcnow())
    job.user = "sshuser"
    session_derecho.add(job)
    session_derecho.commit()

    assert job.pbs_record is None

def test_pbs_record_caching(session_derecho):
    """Verify instance-level caching works."""
    from job_history.models import Job, JobRecord
    import pickle
    import gzip

    class MockPbsRecord:
        def __init__(self):
            self.id = "cached.123"

    pbs_record = MockPbsRecord()
    job = Job(job_id="cached.123", submit=datetime.utcnow())
    job.user = "cacheuser"
    session_derecho.add(job)
    session_derecho.flush()

    compressed = gzip.compress(pickle.dumps(pbs_record))
    job_record = JobRecord(job_id=job.id, compressed_data=compressed)
    session_derecho.add(job_record)
    session_derecho.commit()

    # First access - will decompress
    record1 = job.pbs_record
    # Second access - should use cache
    record2 = job.pbs_record

    # Verify same object returned (cache hit)
    assert record1 is record2
```

## Verification Steps

### Database Schema
1. Run application to trigger `Base.metadata.create_all()`
2. Verify `job_records` table exists with correct columns:
   - `job_id` (INTEGER, PRIMARY KEY)
   - `compressed_data` (BLOB, NOT NULL)
   - `created_at` (DATETIME)
3. Verify foreign key constraint to `jobs.id` with CASCADE delete

### Functional Testing
1. **PBS log import test**:
   ```bash
   # Sync a day of PBS logs
   python -m job_history.cli sync-pbs derecho ./data/sample_pbs_logs/derecho --period 2026-01-29
   ```
   - Verify jobs inserted into `jobs` table
   - Verify corresponding entries in `job_records` table
   - Check compression ratios: `len(compressed_data)` should be ~15-20% of original

2. **Access via property**:
   ```python
   from job_history import get_session, Job

   session = get_session("derecho")
   job = session.query(Job).filter(Job.job_id.like("%.desched1")).first()

   # Should return PbsRecord object (for PBS log imports)
   pbs_record = job.pbs_record
   print(pbs_record)  # Should show PbsRecord object
   print(pbs_record.id, pbs_record.user, pbs_record.queue)
   ```

3. **SSH import test** (verify no JobRecords created):
   ```bash
   # Sync via SSH
   python -m job_history.cli sync derecho --period 2026-01-29
   ```
   - Verify jobs inserted but NO job_records entries

4. **Performance test**:
   - Sync full day (~1000s of jobs)
   - Measure sync time with/without JobRecord creation
   - Should be <10% performance impact
   - Verify compression achieves 60%+ size reduction

### Unit Tests
```bash
pytest tests/test_models.py::test_job_record_round_trip -v
pytest tests/test_models.py::test_job_without_record -v
pytest tests/test_models.py::test_pbs_record_caching -v
```

All tests should pass.

## Critical Files

- **job_history/models.py**: Add `JobRecord` class (after `JobCharge` at line 217) and `Job.pbs_record` property (after `calculate_charges` at line 215)
- **job_history/sync.py**: Modify `_insert_batch()` to populate JobRecords (after line 547)
- **job_history/__init__.py**: Export `JobRecord` model (line 14)
- **tests/test_models.py**: Add unit tests for JobRecord functionality

## Performance Characteristics

- **Storage overhead**: ~400-800 bytes per job (compressed)
- **Sync time impact**: ~5-10ms per job (pickle + gzip)
- **Query impact**: None (lazy loaded only when accessed)
- **Memory usage**: Instance-level cache (no global cache)
- **Compression ratio**: 60-80% size reduction (gzip level 6)

## Backward Compatibility

- **Existing jobs**: Return `None` from `job.pbs_record` (no migration needed)
- **Existing queries**: Work unchanged (JobRecord is optional)
- **Database schema**: New table auto-created, no migration required
- **API compatibility**: No breaking changes to existing code

## Success Criteria

✓ JobRecord model created with correct schema
✓ `job.pbs_record` returns PbsRecord for PBS-log-imported jobs
✓ `job.pbs_record` returns None for SSH-imported jobs
✓ All existing tests pass
✓ New tests verify round-trip integrity
✓ Sync performance impact <10%
✓ Compression achieves 60%+ size reduction
✓ No memory leaks with large result sets
