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

### 3. Implement Additional Parsers

**Current State**: Lustre and POSIX parsers are placeholders

**Goal**: Support multiple filesystem formats

#### 3a. Lustre Parser

**Tasks**:
- [ ] Define Lustre scan log format (work with ops team)
- [ ] Implement `LustreParser.can_parse()` - detect Lustre format
- [ ] Implement `LustreParser.parse_line()` - parse Lustre log entries
- [ ] Add Lustre-specific fields to `ParsedEntry` if needed (stripe_count, OST info)
- [ ] Update parser registry to register LustreParser
- [ ] Create tests in `tests/test_fs_scans_parsers.py`

**Files to Modify**:
- `fs_scans/parsers/lustre.py`
- `fs_scans/parsers/__init__.py` - uncomment registration
- `tests/test_fs_scans_parsers.py` - add Lustre tests

**Estimated Effort**: 8-12 hours (depends on format complexity)

#### 3b. POSIX Parser

**Tasks**:
- [ ] Define POSIX scan log format (JSON? CSV? Custom?)
- [ ] Implement `POSIXParser.can_parse()` - detect format
- [ ] Implement `POSIXParser.parse_line()` - parse entries
- [ ] Update parser registry
- [ ] Create tests

**Files to Modify**:
- `fs_scans/parsers/posix.py`
- `fs_scans/parsers/__init__.py` - uncomment registration
- `tests/test_fs_scans_parsers.py` - add POSIX tests

**Estimated Effort**: 6-10 hours

### 4. Remove Legacy CLI Entry Points

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

### 5. Documentation Updates

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

### 6. Performance Optimization

**Goal**: Improve import and query performance

**Tasks**:
- [ ] Profile import process to identify bottlenecks
- [ ] Consider optimizations:
  - Better batch sizing strategies
  - Parallel query execution
  - Index optimization
  - Memory-mapped I/O tuning

- [ ] Benchmark before/after changes

**Files to Modify**:
- `fs_scans/importers/importer.py`
- `fs_scans/queries/query_engine.py`

**Estimated Effort**: 8-16 hours

### 7. Additional Features

#### 7a. Implement `analyze` Command

**Current State**: Placeholder only

**Goal**: Provide advanced analytics

**Tasks**:
- [ ] Growth trend analysis (compare multiple scans)
- [ ] Duplicate file detection
- [ ] Capacity forecasting
- [ ] Age-based analysis
- [ ] Owner-based chargeback

**Files to Modify**:
- `fs_scans/cli/analyze_cmd.py`
- Create `fs_scans/analysis/` module

**Estimated Effort**: 20-40 hours (depending on features)

#### 7b. Export/Import Capabilities

**Tasks**:
- [ ] Export query results to CSV, JSON, Excel
- [ ] Generate reports in various formats
- [ ] Schedule automated exports

**Estimated Effort**: 6-10 hours

#### 7c. Web Interface

**Tasks**:
- [ ] Create web UI for queries (Flask/FastAPI)
- [ ] Interactive visualizations
- [ ] User management

**Estimated Effort**: 40-80 hours

## Testing Strategy for Future Work

For each enhancement:
1. Write tests first (TDD approach)
2. Update integration tests
3. Run full test suite: `pytest tests/`
4. Verify backward compatibility
5. Performance benchmarking

## Migration Path

When removing legacy entry points:
1. Add deprecation warnings (in code and docs)
2. Email users about upcoming changes
3. Provide migration guide
4. Wait 1-2 months
5. Remove old entry points
6. Delete deprecated code

## Priority Recommendations

**High Priority** (do soon):
1. Complete Importer Extraction (Enhancement #1)
2. Documentation Updates (Enhancement #5)

**Medium Priority** (do within 3-6 months):
3. Complete Query Extraction (Enhancement #2)
4. Implement Lustre Parser (Enhancement #3a)
5. Remove Legacy CLI (Enhancement #4)

**Low Priority** (nice to have):
6. POSIX Parser (Enhancement #3b)
7. Performance Optimization (Enhancement #6)
8. Additional Features (Enhancement #7)

## Notes

- All current functionality is preserved
- Tests are comprehensive (76 passing tests)
- Code is well-structured for incremental improvements
- Can deploy and use immediately while doing enhancements
- Backward compatibility maintained during transition
