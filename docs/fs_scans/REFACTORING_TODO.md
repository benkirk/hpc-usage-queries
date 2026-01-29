# fs_scans Refactoring - Future Enhancements

This document outlines remaining work and future enhancements for the fs_scans refactoring.

## Status: Phase 1-6 Complete ✅

The refactoring is **functionally complete** and all tests pass. The unified CLI is working with:
- Parser architecture (pluggable, extensible)
- Unified CLI (`fs-scans` with subcommands)
- Wrapper scripts for selective deployment
- Backward-compatible imports
- 76 passing tests

## Future Enhancements (Optional)

### 1. Complete Importer Extraction

**Current State**: `importers/importer.py` temporarily imports pass functions from `scan_to_db.py`

**Goal**: Make the importer fully parser-agnostic

**Tasks**:
- [ ] Extract `pass1_discover_directories()` from `scan_to_db.py` to `importers/importer.py`
  - Modify worker functions to accept `parser` parameter
  - Replace direct `parse_line()` calls with `parser.parse_line()`

- [ ] Extract `pass2a_nonrecursive_stats()` from `scan_to_db.py` to `importers/importer.py`
  - Make it work with any parser's `ParsedEntry` format
  - Use `parser.parse_line()` instead of direct calls

- [ ] `pass2b_aggregate_recursive_stats()` is already parser-agnostic (pure SQL)
  - Can be moved as-is or left in place

- [ ] Extract `pass3_populate_summary_tables()` from `scan_to_db.py` to `importers/importer.py`
  - Already mostly parser-agnostic
  - May need minor adjustments

**Files to Modify**:
- `fs_scans/importers/importer.py` - Add the pass function implementations
- `fs_scans/scan_to_db.py` - Remove after extraction complete

**Estimated Effort**: 4-6 hours

### 2. Complete Query Extraction

**Current State**: `cli/query_cmd.py` wraps `query_db.py` main function

**Goal**: Separate CLI from business logic

**Tasks**:
- [ ] Create `queries/query_engine.py`
  - Extract `normalize_path()` function
  - Extract `resolve_path_to_id()` function
  - Extract `get_full_paths_batch()` function
  - Extract `get_directory_counts_batch()` function
  - Extract `execute_query()` function
  - Extract `query_owner_summary()` function

- [ ] Refactor `cli/query_cmd.py`
  - Keep only Click decorators and CLI logic
  - Call functions from `queries/query_engine.py`
  - Maintain all current options

**Files to Modify**:
- `fs_scans/queries/query_engine.py` - Create with extracted logic
- `fs_scans/cli/query_cmd.py` - Simplify to CLI-only
- `fs_scans/query_db.py` - Remove after extraction complete

**Estimated Effort**: 6-8 hours


### 3. Remove Legacy CLI Entry Points

**Current State**: Old CLIs kept for backward compatibility

**Goal**: Clean up deprecated entry points

**Tasks**:
- [ ] Announce deprecation to users (email, docs)
- [ ] Wait for migration period (recommend 1-2 months)
- [ ] Remove old entry points from `pyproject.toml`:
  - `fs-scan-to-db`
  - `query-fs-scan-db`
- [ ] Delete deprecated files (if fully extracted):
  - `fs_scans/scan_to_db.py` (if importer fully extracted)
  - `fs_scans/query_db.py` (if query engine fully extracted)

**Files to Modify**:
- `pyproject.toml` - remove old entry points
- `fs_scans/scan_to_db.py` - delete (optional, if extracted)
- `fs_scans/query_db.py` - delete (optional, if extracted)

**Estimated Effort**: 1-2 hours (plus migration time)

### 4. Documentation Updates

**Current State**: README mentions old CLI commands

**Goal**: Update documentation for new CLI

**Tasks**:
- [ ] Update `fs_scans/README.md`:
  - Replace old CLI examples with new unified CLI
  - Document `--format` option for parser selection
  - Add examples for wrapper scripts
  - Update installation instructions

- [ ] Create architecture documentation:
  - Explain parser architecture
  - Document how to add new parsers
  - Explain multi-pass import algorithm

- [ ] Update inline code comments if needed

**Files to Modify**:
- `fs_scans/README.md`
- `docs/ARCHITECTURE.md` (create new)
- `docs/PARSER_GUIDE.md` (create new)

**Estimated Effort**: 3-4 hours
