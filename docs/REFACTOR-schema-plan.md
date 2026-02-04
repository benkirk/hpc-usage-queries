# Database Schema Optimization Plan

## Goal
Optimize HPC job database for 5-10x faster queries (primary) and ~10-15% size reduction (secondary). Full schema refactor implementing all optimizations at once.

## Current State Analysis

**Database Size:** Derecho: 8.3GB with 10.7M rows, 3,480 users, 1,331 accounts, 150 queues

**Performance Bottlenecks:**
- `v_jobs_charged` view recalculates hours on EVERY query with CASE statements
- Text field comparisons on user/account/queue in WHERE and GROUP BY clauses
- Missing composite indexes for common patterns like (queue, end)
- Daily summary exists but usage_history still does expensive 4-way subquery joins

**Common Query Pattern:** `(queue, end)` filtering used in ALL resource reports from `plots/gen_all.sh`

## Optimization Strategy

### 1. Text Field Normalization (10-15% size reduction, 2-5x GROUP BY speedup)

**Create lookup tables:**
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL
);

CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT UNIQUE NOT NULL
);

CREATE TABLE queues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT UNIQUE NOT NULL
);
```

**Update jobs table:**
- Replace `user TEXT` with `user_id INTEGER` (FK to users)
- Replace `account TEXT` with `account_id INTEGER` (FK to accounts)
- Replace `queue TEXT` with `queue_id INTEGER` (FK to queues)
- Update daily_summary table similarly

**Benefits:**
- Integer joins are 3-4x faster than text joins
- Integer GROUP BY creates smaller temp B-trees
- Storage savings: ~55MB from deduplicated strings
- Better SQLite compression on integers

### 2. Materialized Charging Table (5-10x charging query speedup)

**Replace view with table:**
```sql
CREATE TABLE job_charges (
    job_id INTEGER PRIMARY KEY,
    cpu_hours REAL NOT NULL,
    gpu_hours REAL NOT NULL,
    memory_hours REAL NOT NULL,
    charge_version INTEGER DEFAULT 1,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
```

**Add ORM method for charging:**
```python
class Job(Base):
    def calculate_charges(self, machine: str) -> dict:
        """Single source of truth for charge calculations."""
        job_dict = self.to_dict()
        if machine == 'derecho':
            return derecho_charge(job_dict)  # Uses existing functions
        else:
            return casper_charge(job_dict)
```

**Benefits:**
- Eliminates CASE statement evaluation on every row
- Pre-computed values for instant access
- Flexible: change algorithm and recalculate offline
- Track algorithm version with charge_version field
- Cost: +300MB storage (worth it for 5-10x speedup)

### 3. Composite Index Optimization (3-8x filtered query speedup)

**Add critical indexes based on query patterns:**
```sql
-- Most critical (used in ALL resource reports)
CREATE INDEX ix_jobs_queue_end ON jobs(queue_id, end);

-- User/account aggregations
CREATE INDEX ix_jobs_queue_user_end ON jobs(queue_id, user_id, end);
CREATE INDEX ix_jobs_queue_account_end ON jobs(queue_id, account_id, end);

-- Daily summary optimizations
CREATE INDEX ix_daily_summary_user_date ON daily_summary(user_id, date);
CREATE INDEX ix_daily_summary_account_date ON daily_summary(account_id, date);
CREATE INDEX ix_daily_summary_queue_date ON daily_summary(queue_id, date);
```

**Benefits:**
- Direct index seeks instead of full table scans
- Smaller temp B-trees for GROUP BY
- Better cache utilization
- Cost: +640MB (worth it for 3-8x speedup)

### 4. SQLite Optimization (20-30% improvement)

**Add PRAGMA settings in database.py:**
```python
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")      # Write-ahead logging
    cursor.execute("PRAGMA synchronous=NORMAL")    # Faster writes
    cursor.execute("PRAGMA cache_size=-64000")     # 64MB cache
    cursor.execute("PRAGMA temp_store=MEMORY")     # Temp tables in RAM
    cursor.execute("PRAGMA mmap_size=268435456")   # 256MB memory-mapped I/O
    cursor.execute("PRAGMA optimize")              # Update query planner stats
    cursor.close()
```

## Implementation Steps

### Step 1: Create New Schema (models.py)

**Add new models:**
- `User(Base)` with username field
- `Account(Base)` with account_name field
- `Queue(Base)` with queue_name field
- `JobCharge(Base)` with cpu_hours, gpu_hours, memory_hours, charge_version
- Add `charges` relationship to `Job` model

**Update Job model:**
- Add `user_id`, `account_id`, `queue_id` foreign key columns
- Remove text columns from model (or mark deprecated)
- Add `calculate_charges(machine)` method

**Update DailySummary model:**
- Add `user_id`, `account_id`, `queue_id` foreign key columns
- Update indexes to use integer columns

**Remove:**
- `JobCharged` view model (lines 131-165)

### Step 2: Update Database Initialization (database.py)

**Update `create_views()` to `create_job_charges_table()`:**
- Drop view creation logic
- Create job_charges table instead
- Remove view SQL from charging.py

**Add PRAGMA settings:**
- Create connection event listener with optimization settings
- Add `PRAGMA analyze` to update query planner statistics

**Update `init_db()`:**
- Call `create_job_charges_table()` instead of `create_views()`
- Ensure all foreign keys are enabled

### Step 3: Create Migration Script (new file: migrations/full_refactor.py)

**Migration workflow:**
```python
def migrate_to_optimized_schema(engine, machine):
    """Full schema refactor migration."""
    session = Session(engine)

    # 1. Create new tables
    Base.metadata.create_all(engine)

    # 2. Populate lookup tables
    session.execute("""
        INSERT INTO users (username)
        SELECT DISTINCT user FROM jobs
        WHERE user IS NOT NULL ORDER BY user
    """)
    session.execute("""
        INSERT INTO accounts (account_name)
        SELECT DISTINCT account FROM jobs
        WHERE account IS NOT NULL ORDER BY account
    """)
    session.execute("""
        INSERT INTO queues (queue_name)
        SELECT DISTINCT queue FROM jobs
        WHERE queue IS NOT NULL ORDER BY queue
    """)
    session.commit()

    # 3. Create jobs_v2 with foreign keys
    session.execute("""
        CREATE TABLE jobs_v2 AS
        SELECT
            j.id, j.job_id, j.short_id, j.name,
            u.id as user_id,
            a.id as account_id,
            q.id as queue_id,
            j.status, j.submit, j.eligible, j.start, j.end,
            j.elapsed, j.walltime, j.cputime,
            j.numcpus, j.numgpus, j.numnodes, j.mpiprocs, j.ompthreads,
            j.reqmem, j.memory, j.vmemory,
            j.cputype, j.gputype, j.resources, j.ptargets,
            j.cpupercent, j.avgcpu, j.count
        FROM jobs j
        JOIN users u ON j.user = u.username
        JOIN accounts a ON j.account = a.account_name
        JOIN queues q ON j.queue = q.queue_name
    """)

    # 4. Populate job_charges table
    print("Calculating charges for all jobs...")
    for job in session.query(Job).yield_per(1000):
        charges = job.calculate_charges(machine)
        session.add(JobCharge(
            job_id=job.id,
            **charges,
            charge_version=1
        ))
    session.commit()

    # 5. Replace tables
    session.execute("ALTER TABLE jobs RENAME TO jobs_legacy")
    session.execute("ALTER TABLE jobs_v2 RENAME TO jobs")

    # 6. Recreate indexes
    session.execute("CREATE INDEX ix_jobs_queue_end ON jobs(queue_id, end)")
    session.execute("CREATE INDEX ix_jobs_queue_user_end ON jobs(queue_id, user_id, end)")
    session.execute("CREATE INDEX ix_jobs_queue_account_end ON jobs(queue_id, account_id, end)")
    # ... other indexes ...

    # 7. Update daily_summary similarly
    # ... (same pattern as jobs)

    # 8. Run ANALYZE
    session.execute("ANALYZE")
    session.commit()

    print("Migration complete!")
```

### Step 4: Update Charging Logic (charging.py)

**Add to Job model:**
```python
def calculate_charges(self, machine: str) -> dict:
    """Calculate charging hours for this job."""
    from .charging import derecho_charge, casper_charge

    job_dict = self.to_dict()
    if machine == 'derecho':
        return derecho_charge(job_dict)
    else:
        return casper_charge(job_dict)
```

**Add recalculation utility:**
```python
CURRENT_CHARGE_VERSION = 1

def recalculate_all_charges(engine, machine, batch_size=10000):
    """Recalculate charges when algorithm changes."""
    session = Session(engine)
    offset = 0

    while True:
        jobs = session.query(Job).offset(offset).limit(batch_size).all()
        if not jobs:
            break

        for job in jobs:
            charges = job.calculate_charges(machine)
            job_charge = session.query(JobCharge).filter_by(job_id=job.id).first()
            if job_charge:
                job_charge.cpu_hours = charges['cpu_hours']
                job_charge.gpu_hours = charges['gpu_hours']
                job_charge.memory_hours = charges['memory_hours']
                job_charge.charge_version = CURRENT_CHARGE_VERSION

        session.commit()
        offset += batch_size
        print(f"Processed {offset} jobs...")
```

### Step 5: Update Sync Workflow (sync.py)

**Create JobImporter class:**
```python
class JobImporter:
    """Handle job imports with normalized schema."""

    def __init__(self, session, machine):
        self.session = session
        self.machine = machine

        # Cache lookup tables in memory for fast inserts
        self.user_cache = {}     # username -> id
        self.account_cache = {}  # account_name -> id
        self.queue_cache = {}    # queue_name -> id
        self._load_caches()

    def _load_caches(self):
        """Load lookup tables into memory."""
        for user in self.session.query(User).all():
            self.user_cache[user.username] = user.id
        for account in self.session.query(Account).all():
            self.account_cache[account.account_name] = account.id
        for queue in self.session.query(Queue).all():
            self.queue_cache[queue.queue_name] = queue.id

    def _get_or_create_user(self, username):
        """Get user ID, creating if necessary."""
        if username in self.user_cache:
            return self.user_cache[username]

        user = User(username=username)
        self.session.add(user)
        self.session.flush()
        self.user_cache[username] = user.id
        return user.id

    # Similar for _get_or_create_account and _get_or_create_queue

    def import_job(self, job_data):
        """Import a single job with charges."""
        # Resolve foreign keys
        user_id = self._get_or_create_user(job_data.pop('user'))
        account_id = self._get_or_create_account(job_data.pop('account'))
        queue_id = self._get_or_create_queue(job_data.pop('queue'))

        # Create job with FKs
        job = Job(
            user_id=user_id,
            account_id=account_id,
            queue_id=queue_id,
            **job_data
        )
        self.session.add(job)
        self.session.flush()

        # Calculate and store charges
        charges = job.calculate_charges(self.machine)
        job_charge = JobCharge(job_id=job.id, **charges, charge_version=1)
        self.session.add(job_charge)
```

### Step 6: Update All Queries (queries.py)

**Pattern for updates:**
```python
# Before: Using view and text fields
def usage_by_group(self, resource_type, group_by, start, end):
    query = self.session.query(
        Job.user,
        func.sum(JobCharged.cpu_hours)
    ).join(JobCharged).filter(
        Job.queue.in_(['cpu', 'cpudev'])
    )

# After: Using materialized charges and FKs
def usage_by_group(self, resource_type, group_by, start, end):
    queue_ids = self._get_queue_ids(['cpu', 'cpudev'])

    query = self.session.query(
        User.username.label('user'),
        func.sum(JobCharge.cpu_hours)
    ).join(Job).join(JobCharge).join(User).filter(
        Job.queue_id.in_(queue_ids)
    )
```

**Add helper methods to JobQueries:**
```python
def __init__(self, session, machine):
    self.session = session
    self.machine = machine

    # Cache queue IDs for this machine
    self.cpu_queue_ids = self._get_queue_ids(
        QueryConfig.get_cpu_queues(machine)
    )
    self.gpu_queue_ids = self._get_queue_ids(
        QueryConfig.get_gpu_queues(machine)
    )

def _get_queue_ids(self, queue_names):
    """Convert queue names to IDs."""
    return [
        q.id for q in
        self.session.query(Queue).filter(
            Queue.queue_name.in_(queue_names)
        ).all()
    ]
```

**Update all query methods:**
- `usage_by_group()` (line 277)
- `job_waits_by_resource()` (line 337)
- `job_sizes_by_resource()` (line 424)
- `job_durations()` (line 515)
- `job_memory_per_rank()` (line 572)
- `usage_history()` (line 822)
- `usage_summary()` (line 979)
- `user_summary()` (line 1037)
- All other methods using Job.user, Job.account, Job.queue

### Step 7: Update Daily Summary (summary.py)

**Update SQL to use foreign keys:**
```python
sql = text("""
    INSERT INTO daily_summary (date, user_id, account_id, queue_id, ...)
    SELECT
        date(j.end),
        j.user_id,
        j.account_id,
        j.queue_id,
        COUNT(j.id),
        SUM(jc.cpu_hours),
        SUM(jc.gpu_hours),
        SUM(jc.memory_hours)
    FROM jobs j
    JOIN job_charges jc ON j.id = jc.job_id
    WHERE date(j.end) = :target_date
    GROUP BY date(j.end), j.user_id, j.account_id, j.queue_id
    ON CONFLICT (date, user_id, account_id, queue_id)
    DO UPDATE SET ...
""")
```

## Critical Files to Modify

- `qhist_db/models.py` - Add User/Account/Queue/JobCharge models, update Job/DailySummary
- `qhist_db/database.py` - Add PRAGMA settings, replace view creation with table creation
- `qhist_db/charging.py` - Add Job.calculate_charges() method, add recalculation utility
- `qhist_db/sync.py` - Create JobImporter class with caching and FK resolution
- `qhist_db/queries.py` - Update ALL query methods (25+ methods) to use new schema
- `qhist_db/summary.py` - Update daily summary generation SQL
- `migrations/full_refactor.py` - NEW FILE: Migration script

## Expected Results

**Query Performance:**
| Query Type | Before | After | Speedup |
|------------|--------|-------|---------|
| Pie charts (GROUP BY user/account) | 8-12s | 1-2s | 4-8x |
| Resource history (queue + date filter) | 5-8s | 0.5-1s | 6-10x |
| Job sizes/waits (complex aggregations) | 10-15s | 2-3s | 4-6x |
| Usage summaries (charging calculations) | 12-20s | 1-2s | 8-12x |

**Overall: `plots/gen_all.sh` should run 5-10x faster**

**Database Size:**
- Text normalization: -55 MB
- Materialized charges: +300 MB
- Composite indexes: +640 MB
- SQLite overhead (WAL): +200 MB
- **Net change: +1.1 GB (13% growth) - acceptable for 5-10x speedup**

## Verification

### After Implementation:

1. **Run migration script:**
   ```bash
   python -m qhist_db.migrations.full_refactor --machine derecho
   python -m qhist_db.migrations.full_refactor --machine casper
   ```

2. **Verify row counts match:**
   ```python
   # Check all tables have expected counts
   assert session.query(Job).count() == 10_700_000  # Original count
   assert session.query(JobCharge).count() == 10_700_000  # One per job
   assert session.query(User).count() == 3_480
   assert session.query(Account).count() == 1_331
   assert session.query(Queue).count() == 150
   ```

3. **Verify charging calculations match:**
   ```python
   # Spot-check that new charges match old view calculations
   jobs = session.query(Job).limit(1000).all()
   for job in jobs:
       old_charges = calculate_from_view(job)  # Old view logic
       new_charges = session.query(JobCharge).filter_by(job_id=job.id).first()
       assert abs(old_charges.cpu_hours - new_charges.cpu_hours) < 0.01
   ```

4. **Run plots/gen_all.sh and verify:**
   - All plots generate successfully
   - Results match previous plots (data integrity)
   - Timing shows 5-10x improvement

5. **Test sync workflow:**
   ```bash
   # Sync a new day and verify it populates all tables
   qhist-sync --machine derecho --date 2026-02-01

   # Verify job_charges was populated
   # Verify lookup tables updated if new users/accounts/queues
   ```

6. **Check database size:**
   ```bash
   ls -lh data/derecho.db
   # Should be ~9.4 GB (8.3 GB + 1.1 GB)
   ```

7. **Run EXPLAIN QUERY PLAN on common queries:**
   ```sql
   EXPLAIN QUERY PLAN
   SELECT SUM(jc.cpu_hours)
   FROM jobs j
   JOIN job_charges jc ON j.id = jc.job_id
   WHERE j.queue_id IN (1, 2)
     AND j.end >= '2025-11-01'
     AND j.end <= '2025-11-30';

   -- Should show: SEARCH jobs USING INDEX ix_jobs_queue_end
   ```

## Rollback Plan

If issues arise:
1. Keep `jobs_legacy` table until verification complete
2. Restore from backup if needed
3. Can regenerate database from qhist raw data (slow but safe)
