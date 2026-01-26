# Optimize Parallel Processing in scan_to_db.py

## Problem Statement

With 16 workers, Phase 2a shows workers at ~10% CPU each while the main process is at 102% CPU. The main process is the bottleneck, starving workers of work.

## Root Cause Analysis

1. **Queue serialization overhead**: Chunks of 5000 parsed dicts must be pickled/unpickled
2. **Per-file processing in main process**: ~100M dict lookups and stats accumulations
3. **Small queue buffer**: `maxsize = num_workers * 2` (32 chunks) fills quickly
4. **Single-threaded file I/O**: Main process reads sequentially while managing everything else

## Implementation Plan

### Part 1: Quick Wins (Low Risk)

#### 1a. Increase CHUNK_SIZE
```python
# Before
CHUNK_SIZE = 5000

# After
CHUNK_SIZE = 50000  # 10x larger reduces queue overhead
```

**Rationale**: Larger chunks mean fewer queue operations, less pickle overhead, and workers stay busy longer.

#### 1b. Increase Queue Buffer Size
```python
# Before
input_queue = mp.Queue(maxsize=num_workers * 2)

# After
input_queue = mp.Queue(maxsize=num_workers * 4)  # More buffer room
```

**Rationale**: Allows more chunks to be queued before blocking, smoothing out processing variations.

#### 1c. Use Tuples Instead of Dicts for Queue Data

Replace parsed dict with a lightweight namedtuple or just a tuple:

```python
from typing import NamedTuple

class ParsedFile(NamedTuple):
    path: str
    size: int
    allocated: int
    user_id: int
    atime: int | None
    is_dir: bool

# In worker_parse_lines:
# Instead of: results.append(parsed)
# Use: results.append(ParsedFile(
#     parsed["path"], parsed["size"], parsed["allocated"],
#     parsed["user_id"], parsed["atime"], parsed["is_dir"]
# ))
```

**Rationale**: NamedTuples are ~2-3x faster to pickle than dicts. Also more memory efficient.

### Part 2: Reader Thread (Medium Complexity)

Decouple file reading from queue management and result processing. **Applies to both Phase 1a and Phase 2a** since they have identical structure.

#### Architecture
```
                    +-----------------+
                    |  Reader Thread  |  <-- Reads file, builds chunks
                    |  (I/O bound)    |
                    +--------+--------+
                             |
                             v
                    +--------+--------+
                    |  Chunk Queue    |  <-- Buffer between reader and dispatcher
                    +-----------------+
                             |
                             v
+----------+   +-------------+-------------+   +----------+
| Worker 1 |<--| Main Thread (Dispatcher)  |-->| Worker N |
+----------+   |  - Sends chunks to workers|   +----------+
               |  - Receives parsed results|
               |  - Process results        |
               |  - DB writes              |
               +---------------------------+
```

#### Implementation - Reusable Reader Thread

```python
import threading
from queue import Queue as ThreadQueue

def file_reader_thread(
    input_file: Path,
    chunk_queue: ThreadQueue,
    chunk_size: int,
    line_counter: list[int] | None = None,  # Mutable container for line count
):
    """
    Read file and produce chunks for workers.

    Reusable for both Phase 1a (directory discovery) and Phase 2a (stats).

    Args:
        input_file: Path to input file
        chunk_queue: Thread-safe queue to put chunks into
        chunk_size: Number of lines per chunk
        line_counter: Optional [count] list to track total lines read
    """
    chunk = []
    count = 0
    with open_input_file(input_file) as f:
        for line in f:
            count += 1
            chunk.append(line)
            if len(chunk) >= chunk_size:
                chunk_queue.put(chunk)
                chunk = []
        if chunk:
            chunk_queue.put(chunk)

    if line_counter is not None:
        line_counter[0] = count
    chunk_queue.put(None)  # Sentinel
```

#### Parallel Processing Helper

Create a reusable parallel dispatcher that both phases can use:

```python
def run_parallel_file_processing(
    input_file: Path,
    num_workers: int,
    chunk_size: int,
    filter_type: str,  # "dirs" or "files"
    process_results_fn: Callable,  # Function to process parsed results
    progress_callback: Callable | None = None,
) -> int:
    """
    Generic parallel file processor for Phase 1a and Phase 2a.

    Returns total line count.
    """
    chunk_queue = ThreadQueue(maxsize=num_workers * 4)
    input_queue = mp.Queue(maxsize=num_workers * 4)
    output_queue = mp.Queue()
    line_counter = [0]  # Mutable container for thread communication

    # Start reader thread
    reader = threading.Thread(
        target=file_reader_thread,
        args=(input_file, chunk_queue, chunk_size, line_counter)
    )
    reader.start()

    # Start worker processes
    workers = [
        mp.Process(target=worker_parse_lines, args=(input_queue, output_queue, filter_type))
        for _ in range(num_workers)
    ]
    for w in workers:
        w.start()

    chunks_sent = 0
    chunks_received = 0

    # Main loop: dispatch chunks, process results
    while True:
        chunk = chunk_queue.get()
        if chunk is None:
            break

        input_queue.put(chunk)
        chunks_sent += 1

        # Process available results (non-blocking)
        while True:
            try:
                results = output_queue.get_nowait()
                chunks_received += 1
                process_results_fn(results)
                if progress_callback:
                    progress_callback()
            except Empty:
                break

    # Send sentinels to workers
    for _ in range(num_workers):
        input_queue.put(None)

    # Drain remaining results
    while chunks_received < chunks_sent:
        results = output_queue.get(timeout=5)
        chunks_received += 1
        process_results_fn(results)

    # Cleanup
    reader.join()
    for w in workers:
        w.join()

    return line_counter[0]
```

#### Usage in Phase 1a and Phase 2a

```python
# Phase 1a usage:
def process_dirs(results):
    for parsed in results:
        # Insert to staging table
        ...

line_count = run_parallel_file_processing(
    input_file, num_workers, CHUNK_SIZE, "dirs", process_dirs
)

# Phase 2a usage:
def process_files(results):
    for parsed in results:
        accumulate_file_stats_nr(parsed, path_to_id, pending_updates)

run_parallel_file_processing(
    input_file, num_workers, CHUNK_SIZE, "files", process_files
)
```

**Benefits**:
- Reader thread continuously reads ahead while main thread processes
- Main thread no longer blocked on file I/O
- Better CPU utilization across all threads
- **DRY**: Single implementation serves both phases

## Files to Modify

1. `/Users/benkirk/codes/qhist-queries/fs_scans/scan_to_db.py`

   **Quick Wins:**
   - Update `CHUNK_SIZE` constants (lines 209, 611): 5000 → 50000
   - Update queue maxsize (lines 258, 649): `num_workers * 2` → `num_workers * 4`
   - Add `ParsedFile` namedtuple near top of file
   - Modify `worker_parse_lines` to return `ParsedFile` tuples instead of dicts
   - Modify `accumulate_file_stats_nr` and `process_parsed_dirs` to accept tuples

   **Reader Thread (both phases):**
   - Add `file_reader_thread()` function
   - Add `run_parallel_file_processing()` helper function
   - Refactor `pass1_discover_directories` parallel section to use helper
   - Refactor `pass2_accumulate_stats` parallel section to use helper

## Verification

1. **Correctness**: Run on asp file, compare output DB to baseline
   ```bash
   fs-scan-to-db fs_scans/20260111_csfs1_asp.list.list_all.log --replace -w 16
   query-fs-scan-db asp --summary
   ```

2. **Performance**: Monitor with `top` during Phase 2a
   - Workers should show higher CPU utilization (>50% each ideally)
   - Total throughput should increase

3. **Benchmarks**: Compare before/after runtimes
   ```bash
   time fs-scan-to-db <file> -w 16  # Before
   time fs-scan-to-db <file> -w 16  # After quick wins
   time fs-scan-to-db <file> -w 16  # After reader thread
   ```

## Rollback

Changes are isolated to `scan_to_db.py`. If issues arise:
- Revert CHUNK_SIZE to 5000
- Revert queue maxsize formula
- Remove reader thread, restore original loop structure
