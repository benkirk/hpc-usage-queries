#!/usr/bin/env python3

import pbsparse
from pbsparse import get_pbs_records
import pickle
import gzip
import lz4.frame
import glob
from pathlib import Path

# Find all PBS log directories
log_dirs = sorted(glob.glob("./data/sample_pbs_logs/casper/2025*"))
print(f"Found {len(log_dirs)} directories to process\n")

# Statistics tracking
total_jobs = 0
total_uncompressed_size = 0
total_gzip_size = 0
total_lz4_size = 0
sizes_by_job = []

# Process each directory
for log_dir in log_dirs:
    dir_name = Path(log_dir).name
    print(f"Processing {dir_name}...")

    # Get job end records
    job_ends = get_pbs_records(log_dir, type_filter="E")

    for job in job_ends:
        # Pickle without compression
        pickled_uncompressed = pickle.dumps(job)
        uncompressed_size = len(pickled_uncompressed)

        # Pickle with gzip compression
        pickled_gzip = gzip.compress(pickled_uncompressed)
        gzip_size = len(pickled_gzip)

        # Pickle with lz4 compression
        pickled_lz4 = lz4.frame.compress(pickled_uncompressed)
        lz4_size = len(pickled_lz4)

        # Track statistics
        total_jobs += 1
        total_uncompressed_size += uncompressed_size
        total_gzip_size += gzip_size
        total_lz4_size += lz4_size
        sizes_by_job.append({
            'job_id': job.short_id,
            'uncompressed': uncompressed_size,
            'gzip': gzip_size,
            'lz4': lz4_size,
            'gzip_ratio': gzip_size / uncompressed_size if uncompressed_size > 0 else 0,
            'lz4_ratio': lz4_size / uncompressed_size if uncompressed_size > 0 else 0
        })

print(f"\n{'='*80}")
print(f"SUMMARY - Processed {total_jobs} jobs from {len(log_dirs)} directories")
print(f"{'='*80}")
print(f"Total uncompressed size: {total_uncompressed_size:,} bytes ({total_uncompressed_size/1024/1024:.2f} MiB)")
print(f"Total gzip size:         {total_gzip_size:,} bytes ({total_gzip_size/1024/1024:.2f} MiB)")
print(f"Total lz4 size:          {total_lz4_size:,} bytes ({total_lz4_size/1024/1024:.2f} MiB)")
print(f"\nCompression ratios:")
print(f"  gzip: {total_gzip_size/total_uncompressed_size:.2%} (saves {(total_uncompressed_size - total_gzip_size)/1024/1024:.2f} MiB)")
print(f"  lz4:  {total_lz4_size/total_uncompressed_size:.2%} (saves {(total_uncompressed_size - total_lz4_size)/1024/1024:.2f} MiB)")
print(f"\nlz4 vs gzip: {total_lz4_size/total_gzip_size:.2%} (lz4 is {abs(1-total_lz4_size/total_gzip_size)*100:.1f}% {'smaller' if total_lz4_size < total_gzip_size else 'larger'})")

# Show some examples
if sizes_by_job:
    print(f"\n{'='*80}")
    print(f"SAMPLE (first 10 jobs):")
    print(f"{'='*80}")
    print(f"{'Job ID':<20} {'Uncompressed':>15} {'gzip':>12} {'lz4':>12} {'gzip %':>10} {'lz4 %':>10}")
    print(f"{'-'*80}")
    for item in sizes_by_job[:10]:
        print(f"{item['job_id']:<20} {item['uncompressed']:>15,} {item['gzip']:>12,} {item['lz4']:>12,} {item['gzip_ratio']:>9.1%} {item['lz4_ratio']:>9.1%}")

    # Calculate average compression ratios
    avg_gzip_ratio = sum(item['gzip_ratio'] for item in sizes_by_job) / len(sizes_by_job)
    avg_lz4_ratio = sum(item['lz4_ratio'] for item in sizes_by_job) / len(sizes_by_job)
    print(f"\nAverage compression ratios: gzip={avg_gzip_ratio:.2%}, lz4={avg_lz4_ratio:.2%}")
