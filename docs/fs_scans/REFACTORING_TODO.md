# fs_scans Refactoring: Phases 1-3 Implementation Plan

## Overview

Complete the refactoring by extracting business logic from legacy files (`scan_to_db.py` and `query_db.py`) into the new modular architecture, then removing deprecated CLI entry points.

**Key Challenges:**
1. Field name mismatch: `scan_to_db.py` uses `user_id`, `parsers/base.py` uses `uid`
2. Pass parser instances through multiprocessing workers
3. Move ~2650 lines of code across multiple files
4. Maintain backward compatibility until Phase 3

## Phase 1: Complete Importer Extraction

**Goal:** Extract pass functions from `scan_to_db.py` to `importers/importer.py` and make them parser-agnostic.

### Files to Modify

**Primary Target:** `fs_scans/importers/importer.py`
- Currently: 340 lines with temporary imports from scan_to_db
- After: ~1140 lines with all pass functions extracted

**Source:** `fs_scans/scan_to_db.py` (read-only in Phase 1)

### Implementation Steps

#### 1.1 Extract Helper Functions

Copy from `scan_to_db.py` to `importer.py`:
- `make_empty_update()` (lines 547-554)
- `flush_nr_updates()` (lines 557-615)

These support `pass2a_nonrecursive_stats()`.

#### 1.2 Extract pass1_discover_directories()

**Source:** `scan_to_db.py` lines 307-544

**Key Changes:**
- Add `parser: FilesystemParser` parameter after `input_file`
- Line 427: `parse_line(line)` → `parser.parse_line(line)`
- Lines 410-419: Add `parser=parser` to `run_parallel_file_processing()` call

**Updated Signature:**
```python
def pass1_discover_directories(
    input_file: Path,
    parser: FilesystemParser,  # NEW
    session,
    progress_interval: int = 1_000_000,
    num_workers: int = 1,
) -> tuple[dict[str, int], dict]:
```

#### 1.3 Extract pass2a_nonrecursive_stats()

**Source:** `scan_to_db.py` lines 617-802

**Key Changes:**
- Add `parser: FilesystemParser` parameter after `input_file`
- Line 769: `parse_line(line)` → `parser.parse_line(line)`
- Lines 685, 686, 786: `parsed.user_id` → `parsed.uid` (field name fix)
- Lines 752-761: Add `parser=parser` to `run_parallel_file_processing()` call

**Updated Signature:**
```python
def pass2a_nonrecursive_stats(
    input_file: Path,
    parser: FilesystemParser,  # NEW
    session,
    path_to_id: dict[str, int],
    batch_size: int = 10000,
    progress_interval: int = 1_000_000,
    total_lines: int | None = None,
    num_workers: int = 1,
) -> None:
```

**Critical:** Worker function in `importer.py` already handles `parsed.uid` correctly (line 99).

#### 1.4 Extract pass2b_aggregate_recursive_stats()

**Source:** `scan_to_db.py` lines 804-900

**Changes:** None needed - already parser-agnostic (pure SQL)

Copy verbatim to `importer.py`.

#### 1.5 Extract pass3_populate_summary_tables()

**Source:** `scan_to_db.py` lines 902-1037

**Changes:** None needed - already parser-agnostic

Ensure `pwd` module is imported in `importer.py`.

#### 1.6 Update importer.py run_import()

**Remove temporary imports (lines 220-225):**
```python
# DELETE:
from ..scan_to_db import (
    pass1_discover_directories as _pass1_discover_directories,
    pass2a_nonrecursive_stats as _pass2a_nonrecursive_stats,
    pass2b_aggregate_recursive_stats,
    pass3_populate_summary_tables as _pass3_populate_summary_tables,
)
```

**Update function calls:**
```python
# Line 296: Add parser parameter
path_to_id, metadata = pass1_discover_directories(
    input_file, parser, session, progress_interval, num_workers=workers
)

# Line 302: Add parser parameter
pass2a_nonrecursive_stats(
    input_file,
    parser,
    session,
    path_to_id,
    batch_size,
    progress_interval,
    total_lines=metadata["total_lines"],
    num_workers=workers,
)

# Lines 314, 318: Use local functions (no underscore prefix)
pass2b_aggregate_recursive_stats(session)
pass3_populate_summary_tables(session, input_file, filesystem, metadata)
```

#### 1.7 Add Missing Imports

Ensure `importer.py` has:
```python
from collections import defaultdict
from datetime import datetime
import pwd
from functools import lru_cache
```

### Phase 1 Testing

**Run after Phase 1:**
```bash
# Unit tests
pytest tests/test_importer_extraction.py -v

# Integration tests (existing)
pytest tests/test_fs_scans.py -v

# Test with parallel workers
fs-scans import test_file.log --workers 4
```

**Expected:** All 76 existing tests pass, import performance unchanged.

---

## Phase 2: Complete Query Extraction

**Goal:** Separate query business logic from CLI by creating `queries/query_engine.py`.

### Files to Create

#### New File: `fs_scans/queries/query_engine.py` (~740 lines)

**Extract from query_db.py:**

Core query functions (move as-is):
- `normalize_path()` (lines 41-59)
- `get_all_filesystems()` (lines 62-72)
- `get_scan_date()` (lines 75-95)
- `resolve_path_to_id()` (lines 97-151)
- `get_full_path()` (lines 153-182)
- `get_full_paths_batch()` (lines 184-223)
- `get_directory_counts_batch()` (lines 225-274)
- `query_directories()` (lines 276-428) - core query function
- `get_summary()` (lines 605-631)
- `query_owner_summary()` (lines 633-774)
- `get_username_map()` (lines 777-819)
- `query_single_filesystem()` (lines 876-934)

**Also copy:**
- `_MOUNT_POINT_PREFIXES` constant (lines 33-38)

**Required imports:**
```python
import os
import pwd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from ..core.database import get_data_dir, get_db_path, get_session
from ..core.query_builder import DirectoryQueryBuilder
```

#### New File: `fs_scans/queries/display.py` (~140 lines)

**Extract from query_db.py:**

Display/formatting functions (move as-is):
- `print_results()` (lines 431-568)
- `write_tsv()` (lines 570-603)
- `print_owner_results()` (lines 821-874)

**Required imports:**
```python
from pathlib import Path
from rich.table import Table
from ..cli.common import console, format_datetime, format_size
```

### Files to Modify

#### Modify: `fs_scans/cli/query_cmd.py`

**Current:** 14 lines (just wraps query_db.main)
**After:** ~400 lines (full CLI implementation)

**Strategy:**
1. Remove simple wrapper import
2. Copy Click decorators and main() from query_db.py (lines 940-1447)
3. Update imports to use query_engine and display modules:

```python
from ..queries.query_engine import (
    get_all_filesystems,
    get_scan_date,
    get_summary,
    normalize_path,
    query_directories,
    query_owner_summary,
    query_single_filesystem,
    get_username_map,
)
from ..queries.display import (
    print_results,
    print_owner_results,
    write_tsv,
)
```

4. Keep all Click decorators and CLI logic unchanged
5. Update implementation to call imported functions

#### Update: `fs_scans/queries/__init__.py`

Add exports:
```python
from .query_engine import (
    query_directories,
    query_owner_summary,
    get_username_map,
    # ... etc
)
from .display import (
    print_results,
    write_tsv,
    print_owner_results,
)
```

#### Update: `fs_scans/query_db.py`

Add deprecation warning at top:
```python
"""
DEPRECATED: This module is deprecated. Use the new CLI instead:
  - Old: query-fs-scan-db [OPTIONS]
  - New: fs-scans query [OPTIONS]

This module will be removed in a future release.
"""
import warnings
warnings.warn(
    "query_db.py is deprecated. Use 'fs-scans query' instead.",
    DeprecationWarning,
    stacklevel=2
)
```

Keep file for backward compatibility (delete in Phase 3).

### Phase 2 Testing

**Run after Phase 2:**
```bash
# Unit tests
pytest tests/test_query_engine.py -v

# Integration tests
pytest tests/test_queries.py -v

# CLI tests
fs-scans query asp --limit 10
fs-scans query all --group-by owner
fs-scans-query asp --limit 10

# Old CLI (should still work with warning)
query-fs-scan-db asp --limit 10
```

**Expected:** All tests pass, output identical to old implementation.

---

## Phase 3: Remove Legacy CLI Entry Points

**Goal:** Clean up deprecated files and entry points immediately (no backward compatibility concerns).

### Prerequisites

1. **Phase 1 complete** ✓
2. **Phase 2 complete** ✓
3. **All tests passing**

### Implementation Steps

#### 3.1 Remove Entry Points

**Edit:** `pyproject.toml`

**Remove lines 40-41:**
```toml
# DELETE THESE:
fs-scan-to-db = "fs_scans.scan_to_db:main"
query-fs-scan-db = "fs_scans.query_db:main"
```

**Result:**
```toml
[project.scripts]
qhist-sync = "scripts.sync_jobs:main"
qhist-report = "qhist_db.cli:cli"

# New unified CLI
fs-scans = "fs_scans.cli.main:fs_scans_cli"

# Convenience wrappers (allow selective deployment)
fs-scans-import = "fs_scans.wrappers.fs_scans_import:main"
fs-scans-query = "fs_scans.wrappers.fs_scans_query:main"
fs-scans-analyze = "fs_scans.wrappers.fs_scans_analyze:main"
```

#### 3.2 Verify No Dependencies

**Before deletion, check:**
```bash
# Should find NO imports outside of the files themselves
grep -r "from.*scan_to_db import" fs_scans/ --exclude-dir=__pycache__
grep -r "from.*query_db import" fs_scans/ --exclude-dir=__pycache__
```

**Expected:** No matches (all imports removed in Phases 1-2)

#### 3.3 Delete Deprecated Files

```bash
rm fs_scans/scan_to_db.py      # 1203 lines (Phase 1 extraction complete)
rm fs_scans/query_db.py        # 1450 lines (Phase 2 extraction complete)
```

#### 3.4 Update Documentation

**Edit:** `docs/fs_scans/REFACTORING_TODO.md`
- Mark Phases 1-3 as "Complete ✅"
- Update status section

**Edit:** `fs_scans/README.md` (if exists)
- Update CLI examples to use `fs-scans` commands
- Remove references to old CLIs

### Phase 3 Testing

**After deletion:**
```bash
# Reinstall package
pip install -e .

# Verify old CLIs gone
! command -v fs-scan-to-db      # Should FAIL (expected)
! command -v query-fs-scan-db   # Should FAIL (expected)

# Verify new CLIs work
fs-scans --help
fs-scans import --help
fs-scans query --help

# Run full test suite
pytest tests/ -v
```

**Expected:** Old CLIs unavailable, new CLIs work perfectly, all tests pass.

---

## Implementation Order

**Recommended Sequential Approach:**

1. **Step 1:** Phase 1 - Importer extraction
   - Extract pass functions to importer.py
   - Make parser-agnostic
   - Test thoroughly

2. **Step 2:** Phase 2 - Query engine extraction
   - Create query_engine.py and display.py
   - Update cli/query_cmd.py
   - Test thoroughly

3. **Step 3:** Phase 3 - Immediate cleanup (no migration period needed)
   - Remove entry points from pyproject.toml
   - Delete deprecated files
   - Update documentation
   - Test complete system

**Alternative Parallel Approach:**
- Phases 1 and 2 can be done in parallel (independent)
- Phase 3 executes immediately after both complete

**Timeline:** All phases can be completed in a single session (~2-3 hours)

---

## Critical Files Summary

### Phase 1
- **Primary:** `fs_scans/importers/importer.py` (modify: add ~800 lines)
- **Source:** `fs_scans/scan_to_db.py` (read-only)
- **Reference:** `fs_scans/parsers/base.py` (for ParsedEntry.uid)

### Phase 2
- **New:** `fs_scans/queries/query_engine.py` (create: ~740 lines)
- **New:** `fs_scans/queries/display.py` (create: ~140 lines)
- **Modify:** `fs_scans/cli/query_cmd.py` (expand: 14 → 400 lines)
- **Source:** `fs_scans/query_db.py` (read-only)

### Phase 3
- **Delete:** `fs_scans/scan_to_db.py`
- **Delete:** `fs_scans/query_db.py`
- **Modify:** `pyproject.toml` (remove 2 entry points)
- **Update:** Documentation files

---

## Key Risks and Mitigation

### Risk 1: Field Name Inconsistency
- **Issue:** `user_id` vs `uid` mismatch
- **Mitigation:** Global search/replace, update all references
- **Test:** Unit tests verify correct field access

### Risk 2: Multiprocessing Pickle Errors
- **Issue:** Parser must be picklable for workers
- **Mitigation:** Parser classes are simple dataclasses (picklable)
- **Test:** Integration test with `--workers 4`

### Risk 3: Import Circular Dependencies
- **Issue:** Module import order problems
- **Mitigation:** query_engine only imports from core/, not vice versa
- **Test:** `python -c "import fs_scans.queries.query_engine"`

### Risk 4: Missing Imports
- **Issue:** Forgot to copy import statements
- **Mitigation:** Careful review of all imports
- **Test:** Static type checking, run full test suite

### Risk 5: Incomplete Extraction
- **Issue:** Missed functions or dependencies
- **Mitigation:** Comprehensive grep search, thorough testing
- **Test:** Full test suite after each phase

---

## Verification Steps

### After Phase 1
```bash
# Import test
pytest tests/test_fs_scans.py::test_import -v

# Parallel workers test
fs-scans import test_file.log --workers 4

# Check field mapping
grep -r "user_id" fs_scans/importers/  # Should find NONE
```

### After Phase 2
```bash
# Query test
fs-scans query asp --limit 10

# Old CLI (should work with warning)
query-fs-scan-db asp --limit 10

# Import test
python -c "from fs_scans.queries import query_engine"
```

### After Phase 3
```bash
# Installation test
pip install -e .

# Old CLIs should be gone
! command -v fs-scan-to-db
! command -v query-fs-scan-db

# New CLIs should work
fs-scans --help
fs-scans import --help
fs-scans query --help

# Full test suite
pytest tests/ -v --tb=short
```

### End-to-End Test
```bash
# Test complete workflow
fs-scans import tests/fixtures/sample_scan.log --workers 4
fs-scans query asp --limit 10 --group-by owner
fs-scans-import tests/fixtures/sample_scan.log
fs-scans-query asp --limit 10

# Performance check
time fs-scans import large_scan.log
# Should match baseline (±5%)
```

---

## Success Criteria

- [ ] All 76 existing tests pass after each phase
- [ ] Import performance unchanged (±5%)
- [ ] Query performance unchanged (±5%)
- [ ] Old CLIs removed in Phase 3
- [ ] New CLIs work correctly
- [ ] No imports of deleted modules
- [ ] Documentation updated
- [ ] Code is parser-agnostic
- [ ] Field names consistent (uid, not user_id)
