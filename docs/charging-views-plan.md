# Charging Views & Daily Summary Plan

## Goals

1. Create SQL views with virtual charging columns (node_hours, cpu_hours, gpu_hours, memory_hours)
2. Support different charging schemes by machine and queue
3. Create daily summary tables for fast retrieval of charges per user/account/queue
4. Optimize sync to skip already-processed days

## Charging Formulas

### Derecho

| Queue Type | Formula | Unit |
|------------|---------|------|
| CPU queues (default) | `elapsed * numnodes * 128 / 3600` | core-hours |
| GPU queues | `elapsed * numnodes * 4 / 3600` | GPU-hours |
| Development (develop) | `elapsed * numcpus / 3600` | core-hours |
| GPU Development (dgpudev) | `elapsed * numgpus / 3600` | GPU-hours |

### Casper

Casper tracks both metrics for all jobs:

| Metric | Formula |
|--------|---------|
| CPU-hours | `elapsed * numcpus / 3600` |
| Memory-hours | `elapsed * memory_gb / 3600` |

## Implementation

### 1. Charging Configuration (`qhist_db/charging.py`)

New module defining charging rules per machine and queue:

```python
DERECHO_RULES = {
    "default": lambda j: j.elapsed * j.numnodes * 128 / 3600,
    "gpu": lambda j: j.elapsed * j.numnodes * 4 / 3600,
    "develop": lambda j: j.elapsed * j.numcpus / 3600,
    "dgpudev": lambda j: j.elapsed * j.numgpus / 3600,
}

CASPER_RULES = {
    "default": lambda j: {
        "cpu_hours": j.elapsed * j.numcpus / 3600,
        "memory_hours": j.elapsed * (j.memory or 0) / (3600 * 1024**3),
    }
}
```

### 2. SQL Views (`v_jobs_charged`)

Machine-specific views with computed charging columns:

**Derecho:**
```sql
CREATE VIEW v_jobs_charged AS
SELECT *,
  CASE
    WHEN queue LIKE '%gpu%' AND queue NOT LIKE '%dev%'
      THEN elapsed * numnodes * 4 / 3600.0
    WHEN queue LIKE '%dev%' AND queue LIKE '%gpu%'
      THEN elapsed * COALESCE(numgpus, 0) / 3600.0
    WHEN queue LIKE '%dev%'
      THEN elapsed * COALESCE(numcpus, 0) / 3600.0
    ELSE elapsed * numnodes * 128 / 3600.0
  END AS charge_hours
FROM jobs;
```

**Casper:**
```sql
CREATE VIEW v_jobs_charged AS
SELECT *,
  elapsed * COALESCE(numcpus, 0) / 3600.0 AS cpu_hours,
  elapsed * COALESCE(memory, 0) / (3600.0 * 1024 * 1024 * 1024) AS memory_hours
FROM jobs;
```

### 3. Daily Summary Table

New table aggregating charges per (date, user, account, queue):

```python
class DailySummary(Base):
    __tablename__ = "daily_summary"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    user = Column(Text, nullable=False)
    account = Column(Text, nullable=False)
    queue = Column(Text, nullable=False)

    job_count = Column(Integer, default=0)
    charge_hours = Column(Float, default=0)    # Derecho
    cpu_hours = Column(Float, default=0)       # Casper
    memory_hours = Column(Float, default=0)    # Casper

    __table_args__ = (
        UniqueConstraint("date", "user", "account", "queue"),
        Index("ix_daily_summary_date", "date"),
    )
```

### 4. Summary Generation (`qhist_db/summary.py`)

- `generate_daily_summary(session, machine, date)` - Aggregate from v_jobs_charged
- `get_summarized_dates(session)` - Return set of dates already processed

### 5. Smart Sync

Update `sync_jobs_bulk()`:

1. Before fetching, check if date exists in daily_summary
2. Skip days already summarized (unless `--force`)
3. After syncing a day, generate its summary
4. New CLI flags: `--force`, `--summary-only`

## File Changes

| File | Action |
|------|--------|
| `qhist_db/charging.py` | New - Charging rules |
| `qhist_db/summary.py` | New - Summary generation |
| `qhist_db/models.py` | Edit - Add DailySummary model |
| `qhist_db/database.py` | Edit - Add view creation |
| `qhist_db/sync.py` | Edit - Smart skip logic |
| `scripts/sync_jobs.py` | Edit - New CLI flags |
| `README.md` | Edit - Document new features |
| `docs/schema.md` | Edit - Document new tables/views |
