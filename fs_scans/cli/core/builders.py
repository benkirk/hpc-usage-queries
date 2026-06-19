"""Envelope builders for the fs-scans CLI.

Each builder is a pure function that turns a :class:`FsScanQueries` result
plus its display metadata into a ``kind=``-tagged envelope dict. The JSON
exporter serializes the envelope directly; the rich/tsv exporters dispatch on
``kind`` and reuse the existing presentation code in
:mod:`fs_scans.queries.display` and the histogram ``format_output`` methods,
so console output is byte-identical to the pre-refactor CLI.

Envelope shape (tabular kinds)::

    {
      "kind": "fs_directories" | "fs_owner_summary" | "fs_group_summary",
      "filesystems": [...],
      "scan_dates": [date, ...],
      "columns": [{"key", "header", "format"}, ...],   # hint for JSON consumers
      "rows": [ {...}, ... ],                            # the data
      "meta": { ... rendering hints ... },
    }

Histogram kinds carry the serialized histogram under ``"histogram"`` instead
of ``columns``/``rows``.
"""

# Column specs (key/header/format) describing the data columns. These give
# JSON consumers a stable, ordered view; rich rendering uses display.py.
_DIR_COLUMNS = [
    {"key": "path", "header": "Directory", "format": "s"},
    {"key": "depth", "header": "Depth", "format": ""},
    {"key": "total_size_r", "header": "Size", "format": ""},
    {"key": "total_size_nr", "header": "Size (NR)", "format": ""},
    {"key": "file_count_r", "header": "Files", "format": ""},
    {"key": "file_count_nr", "header": "Files (NR)", "format": ""},
    {"key": "dir_count_r", "header": "Dirs", "format": ""},
    {"key": "dir_count_nr", "header": "Dirs (NR)", "format": ""},
    {"key": "max_atime_r", "header": "Atime", "format": ""},
    {"key": "max_atime_nr", "header": "Atime (NR)", "format": ""},
    {"key": "owner_uid", "header": "Owner", "format": ""},
]

_OWNER_COLUMNS = [
    {"key": "owner_uid", "header": "Owner", "format": ""},
    {"key": "filesystem", "header": "Filesystem", "format": "s"},
    {"key": "total_size", "header": "Total Size", "format": ""},
    {"key": "total_files", "header": "Total Files", "format": ""},
    {"key": "directory_count", "header": "Directories", "format": ""},
]

_GROUP_COLUMNS = [
    {"key": "owner_gid", "header": "Group", "format": ""},
    {"key": "filesystem", "header": "Filesystem", "format": "s"},
    {"key": "total_size", "header": "Total Size", "format": ""},
    {"key": "total_files", "header": "Total Files", "format": ""},
    {"key": "directory_count", "header": "Directories", "format": ""},
]


def _scan_dates(filesystems, scan_dates):
    return {
        "filesystems": list(filesystems),
        "scan_dates": list(scan_dates) if scan_dates else [],
    }


def build_directories(
    rows,
    *,
    filesystems,
    scan_dates=None,
    username_map=None,
    verbose=False,
    leaves_only=False,
    show_total=False,
    show_dir_counts=False,
):
    """Envelope for the directory-listing query (``fs-scans query``)."""
    return {
        "kind": "fs_directories",
        **_scan_dates(filesystems, scan_dates),
        "columns": _DIR_COLUMNS,
        "rows": rows,
        "meta": {
            "username_map": username_map or {},
            "verbose": verbose,
            "leaves_only": leaves_only,
            "show_total": show_total,
            "show_dir_counts": show_dir_counts,
        },
    }


def build_owner_summary(
    rows, *, filesystems, scan_dates=None, name_map=None, show_filesystem=False
):
    """Envelope for ``fs-scans query --group-by owner``."""
    return {
        "kind": "fs_owner_summary",
        **_scan_dates(filesystems, scan_dates),
        "columns": _OWNER_COLUMNS,
        "rows": rows,
        "meta": {
            "name_map": name_map or {},
            "show_filesystem": show_filesystem,
        },
    }


def build_group_summary(
    rows, *, filesystems, scan_dates=None, name_map=None, show_filesystem=False
):
    """Envelope for ``fs-scans query --group-by group``."""
    return {
        "kind": "fs_group_summary",
        **_scan_dates(filesystems, scan_dates),
        "columns": _GROUP_COLUMNS,
        "rows": rows,
        "meta": {
            "name_map": name_map or {},
            "show_filesystem": show_filesystem,
        },
    }


def build_access_history(histogram, *, filesystems, top_n=10):
    """Envelope for ``fs-scans analyze --access-history``."""
    return {
        "kind": "fs_access_history",
        "filesystems": list(filesystems),
        "histogram": histogram,
        "meta": {"top_n": top_n},
    }


def build_file_size(histogram, *, filesystems, top_n=10):
    """Envelope for ``fs-scans analyze --file-size``."""
    return {
        "kind": "fs_file_size",
        "filesystems": list(filesystems),
        "histogram": histogram,
        "meta": {"top_n": top_n},
    }
