# GPFS Scan Database - SQLite/SQLAlchemy Approach

## Summary
Extend the GPFS parser to persist directory statistics in a SQLite database using SQLAlchemy ORM, following the patterns established in `qhist_db`. This enables persistent storage, efficient querying, and integration with future "cs-queries" tooling.

## Database Schema

### Table 1: `directories` (path hierarchy)
```sql
CREATE TABLE directories (
    dir_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id    INTEGER REFERENCES directories(dir_id),
    path         TEXT NOT NULL UNIQUE,
    depth        INTEGER NOT NULL,

    INDEX ix_directories_parent (parent_id),
    INDEX ix_directories_depth (depth)
);
```

### Table 2: `directory_stats` (metrics)
```sql
CREATE TABLE directory_stats (
    dir_id           INTEGER PRIMARY KEY REFERENCES directories(dir_id),

    -- Non-recursive (direct children)
    file_count_nr    INTEGER DEFAULT 0,
    total_size_nr    INTEGER DEFAULT 0,
    max_atime_nr     TIMESTAMP,

    -- Recursive (all descendants)
    file_count_r     INTEGER DEFAULT 0,
    total_size_r     INTEGER DEFAULT 0,
    max_atime_r      TIMESTAMP,

    -- Single-owner tracking (-1 = no files, NULL = multiple owners)
    owner_uid        INTEGER DEFAULT -1,

    INDEX ix_stats_owner (owner_uid),
    INDEX ix_stats_size_r (total_size_r)
);
```

## Multi-Pass Algorithm

### Pass 1: Directory Discovery
Scan log file, extract all unique directory paths (from file entries and explicit directory entries):
```python
def pass1_discover_directories(input_file, session):
    """First pass: identify all directories and build hierarchy."""
    seen_paths = set()

    for line in stream_log(input_file):
        parsed = parse_line(line)
        if not parsed:
            continue

        # Walk up from file's parent to root, collecting all dirs
        current = os.path.dirname(parsed["path"])
        while current and current != "/" and current not in seen_paths:
            seen_paths.add(current)
            current = os.path.dirname(current)

    # Batch insert directories (sorted by depth for parent resolution)
    insert_directories(session, sorted(seen_paths, key=lambda p: p.count("/")))
```

### Pass 2: Statistics Accumulation
Re-scan log file, accumulating stats with batched database updates:
```python
def pass2_accumulate_stats(input_file, session, batch_size=10000):
    """Second pass: accumulate file statistics into directory_stats."""
    pending_updates = defaultdict(lambda: {"nr": DirDelta(), "r": DirDelta()})

    for line in stream_log(input_file):
        parsed = parse_line(line)
        if not parsed or parsed["is_dir"]:
            continue

        parent = os.path.dirname(parsed["path"])

        # Non-recursive: direct parent only
        pending_updates[parent]["nr"].add(parsed)

        # Recursive: all ancestors
        current = parent
        while current and current != "/":
            pending_updates[current]["r"].add(parsed)
            current = os.path.dirname(current)

        # Flush batch periodically
        if sum(len(v) for v in pending_updates.values()) >= batch_size:
            flush_updates(session, pending_updates)
            pending_updates.clear()

    flush_updates(session, pending_updates)
```

### Batched Update Strategy
```python
def flush_updates(session, pending_updates):
    """Apply accumulated deltas to database."""
    for path, deltas in pending_updates.items():
        # Use UPDATE with increments, not full replacement
        session.execute(text("""
            UPDATE directory_stats SET
                file_count_nr = file_count_nr + :nr_count,
                total_size_nr = total_size_nr + :nr_size,
                max_atime_nr = MAX(max_atime_nr, :nr_atime),
                file_count_r = file_count_r + :r_count,
                total_size_r = total_size_r + :r_size,
                max_atime_r = MAX(max_atime_r, :r_atime),
                owner_uid = CASE
                    WHEN owner_uid = -1 THEN :uid
                    WHEN owner_uid != :uid THEN NULL
                    ELSE owner_uid
                END
            WHERE dir_id = (SELECT dir_id FROM directories WHERE path = :path)
        """), {...})
    session.commit()
```

## Files to Create

| File | Purpose |
|------|---------|
| `fs_scans/models.py` | SQLAlchemy ORM models (Directory, DirectoryStats) |
| `fs_scans/database.py` | Engine/session management, init_db() |
| `fs_scans/scan_to_db.py` | Multi-pass import CLI |
| `fs_scans/query_db.py` | Query CLI for database |

## Database Organization
- **Separate DB per filesystem**: `fs_scans/asp.db`, `fs_scans/cisl.db`, etc.
- Follows `qhist_db` pattern of per-machine databases
- Filesystem name extracted from input filename (e.g., `20260111_csfs1_asp.list...` → `asp`)

## Implementation Details

### 1. `fs_scans/models.py`
```python
from sqlalchemy import Column, Integer, BigInteger, Text, DateTime, ForeignKey, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Directory(Base):
    __tablename__ = "directories"

    dir_id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey("directories.dir_id"), nullable=True)
    path = Column(Text, nullable=False, unique=True, index=True)
    depth = Column(Integer, nullable=False, index=True)

    stats = relationship("DirectoryStats", back_populates="directory", uselist=False)

class DirectoryStats(Base):
    __tablename__ = "directory_stats"

    dir_id = Column(Integer, ForeignKey("directories.dir_id"), primary_key=True)

    # Non-recursive
    file_count_nr = Column(BigInteger, default=0)
    total_size_nr = Column(BigInteger, default=0)
    max_atime_nr = Column(DateTime)

    # Recursive
    file_count_r = Column(BigInteger, default=0)
    total_size_r = Column(BigInteger, default=0)
    max_atime_r = Column(DateTime)

    # Owner tracking: -1=no files yet, NULL=multiple owners, else=single owner UID
    owner_uid = Column(Integer, default=-1, index=True)

    directory = relationship("Directory", back_populates="stats")

    __table_args__ = (
        Index("ix_stats_size_r", "total_size_r"),
        Index("ix_stats_files_r", "file_count_r"),
    )
```

### 2. `fs_scans/database.py`
```python
def get_db_path(filesystem: str) -> Path:
    """Return path to database file for a filesystem."""
    return Path(__file__).parent / f"{filesystem}.db"

def get_engine(filesystem: str):
    db_path = get_db_path(filesystem)
    return create_engine(f"sqlite:///{db_path}")

def get_session(filesystem: str):
    engine = get_engine(filesystem)
    Session = sessionmaker(bind=engine)
    return Session()

def init_db(filesystem: str):
    engine = get_engine(filesystem)
    Base.metadata.create_all(engine)
    return engine
```

### 3. Import CLI (`scan_to_db.py`)
```bash
python fs_scans/scan_to_db.py <input_file> [options]

Options:
  --db PATH              Override database path (default: auto from filename)
  --batch-size N         Batch size for DB updates (default: 10000)
  --progress-interval N  Progress reporting (default: 1M lines)
  --replace              Drop and recreate tables before import
```

### 4. Query CLI (`query_db.py`)
```bash
python fs_scans/query_db.py <filesystem> [options]

Options:
  -d, --min-depth N      Filter by minimum path depth
  -s, --single-owner     Only show single-owner directories
  -u, --owner-id UID     Filter to specific owner
  -n, --limit N          Limit results
  --sort-by FIELD        Sort by: size_r, size_nr, files_r, files_nr, atime_r, path
  --path-prefix PATH     Filter to paths starting with prefix
  -o, --output FILE      Write TSV output to file
```

## Verification
1. Import 10K line sample: `python fs_scans/scan_to_db.py /tmp/test_sample.log`
2. Query and compare with in-memory parser output
3. Run on full asp file, verify totals match
4. Test query filters and sorting
5. Benchmark query performance vs in-memory approach
