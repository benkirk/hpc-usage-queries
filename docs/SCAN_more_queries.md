# Implementation Plan: New Tables and Query Features for fs_scans

## Overview

Add three new ORM tables and two new query CLI features to the fs_scans module.

## New Tables

### 1. ScanMetadata
Track scan provenance and aggregate totals:
```python
class ScanMetadata(Base):
    __tablename__ = "scan_metadata"
    scan_id = Column(Integer, primary_key=True, autoincrement=True)
    source_file = Column(Text, nullable=False)      # e.g., "20260111_csfs1_asp.list.list_all.log"
    scan_timestamp = Column(DateTime)               # parsed from YYYYMMDD in filename
    import_timestamp = Column(DateTime)             # when imported
    filesystem = Column(Text, nullable=False)
    total_directories = Column(BigInteger, default=0)
    total_files = Column(BigInteger, default=0)
    total_size = Column(BigInteger, default=0)
```

### 2. OwnerSummary
Pre-computed per-owner aggregates (makes `--group-by owner` instant):
```python
class OwnerSummary(Base):
    __tablename__ = "owner_summary"
    owner_uid = Column(Integer, primary_key=True)
    total_size = Column(BigInteger, default=0)
    total_files = Column(BigInteger, default=0)
    directory_count = Column(Integer, default=0)
```

### 3. UserInfo
Cache UID-to-username mappings resolved during scan:
```python
class UserInfo(Base):
    __tablename__ = "user_info"
    uid = Column(Integer, primary_key=True)
    username = Column(Text)
    full_name = Column(Text)  # GECOS field
```

## New CLI Features

### 1. `--name-pattern` / `-N`
Filter directories by name using SQL GLOB patterns:
```bash
query-fs-scan-db --name-pattern "*scratch*"
query-fs-scan-db -N "tmp*"
```

### 2. `--group-by owner`
Show per-user summary (uses OwnerSummary table when no filters):
```bash
query-fs-scan-db --group-by owner
query-fs-scan-db --group-by owner -d 4 -P /gpfs/csfs1/asp
```

## Files to Modify

| File | Changes |
|------|---------|
| `fs_scans/models.py` | Add ScanMetadata, OwnerSummary, UserInfo classes |
| `fs_scans/database.py` | Add `extract_scan_timestamp()` helper |
| `fs_scans/scan_to_db.py` | Add Phase 3 to populate summary tables after Phase 2b |
| `fs_scans/query_db.py` | Add `--name-pattern` and `--group-by owner` options |

## Implementation Details

### Phase 3 in scan_to_db.py (after Phase 2b)

Insert after `pass2b_aggregate_recursive_stats()` at line 1024:

1. **Phase 3a**: Populate UserInfo
   - Collect UIDs during Phase 2a file processing
   - Resolve via `pwd.getpwuid()` with `@lru_cache`
   - INSERT OR REPLACE into user_info table

2. **Phase 3b**: Compute OwnerSummary
   ```sql
   INSERT INTO owner_summary (owner_uid, total_size, total_files, directory_count)
   SELECT owner_uid, SUM(total_size_r), SUM(file_count_r), COUNT(*)
   FROM directory_stats
   WHERE owner_uid IS NOT NULL AND owner_uid != -1
   GROUP BY owner_uid
   ```

3. **Phase 3c**: Record ScanMetadata
   - Parse scan_timestamp from filename (YYYYMMDD)
   - Record import_timestamp as datetime.now()
   - Aggregate totals from root directories

### query_db.py Changes

1. **--name-pattern**: Add condition to `query_directories()`:
   ```python
   if name_pattern is not None:
       conditions.append("d.name GLOB :name_pattern")
       params["name_pattern"] = name_pattern
   ```

2. **--group-by owner**: New query function with two paths:
   - **Fast path**: Query OwnerSummary table (no filters)
   - **Dynamic path**: Compute on-the-fly when filters applied

## Implementation Order

1. `models.py` - Define new ORM classes
2. `database.py` - Add `extract_scan_timestamp()` helper
3. `scan_to_db.py` - Add UID collection and Phase 3 processing
4. `query_db.py` - Add CLI options and query functions
5. Update existing databases with new indexes

## Verification

1. **Scan import test**:
   ```bash
   fs-scan-to-db fs_scans/20260125_csfs1_mmm.list.list_all.log
   sqlite3 fs_scans/data/mmm.db "SELECT * FROM scan_metadata; SELECT COUNT(*) FROM owner_summary; SELECT COUNT(*) FROM user_info;"
   ```

2. **Query tests**:
   ```bash
   query-fs-scan-db --name-pattern "*scratch*" -n 10
   query-fs-scan-db --group-by owner -n 20
   query-fs-scan-db --group-by owner -d 4 -P /gpfs/csfs1/cisl
   ```

3. **Backward compatibility**: Existing databases without new tables should still work for standard queries
