"""High-level query facade for filesystem scan databases.

``FsScanQueries`` is the single source of truth for the data behind the
``fs-scans query`` and ``fs-scans analyze`` commands.  It owns the
multi-filesystem orchestration (parallel fan-out, cross-database
aggregation, scan-date collection, UID/GID name resolution, histogram
fast/slow paths) that previously lived inline in the Click command bodies,
and returns plain Python dicts so that:

  * the CLI can wrap results in ``kind=`` envelopes and render them via the
    :mod:`fs_scans.cli.core` exporters, and
  * external Python applications can ``from fs_scans import FsScanQueries``
    and consume the same results directly — mirroring how ``job_history``
    exposes :class:`~job_history.queries.JobQueries`.

The class is backend-agnostic: session creation flows through the
backend-aware :func:`fs_scans.core.database.get_session`, so it works over
both the SQLite ``*.db`` files and the CNPG/PostgreSQL backend unchanged.

Sessions are opened and closed internally per call (fs_scans is
multi-*filesystem* by default, unlike ``job_history`` which threads a single
session), so callers never manage session lifecycle.
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from ..core.database import get_session, set_data_dir
from ..core.models import ATIME_BUCKETS, SIZE_BUCKETS
from .access_history import (
    AccessHistogram,
    compute_access_history,
)
from .file_size import compute_size_histogram_from_directory_stats
from .histogram_common import (
    HistogramData,
    aggregate_histograms_across_databases,
)
from .query_engine import (
    get_all_filesystems,
    get_scan_date,
    get_summary,
    normalize_path,
    query_directories,
    query_group_summary,
    query_owner_summary,
    query_single_filesystem,
    resolve_groupnames_across_databases,
    resolve_usernames_across_databases,
)


# Sort keys for the combined multi-filesystem directory listing. Mirrors the
# logic that previously lived in fs_scans/cli/query_cmd.py.
_DIR_SORT_KEYS = {
    "size": lambda d: d["total_size_r"] or 0,
    "size_r": lambda d: d["total_size_r"] or 0,
    "size_nr": lambda d: d["total_size_nr"] or 0,
    "files": lambda d: d["file_count_r"] or 0,
    "files_r": lambda d: d["file_count_r"] or 0,
    "files_nr": lambda d: d["file_count_nr"] or 0,
    "dirs": lambda d: d["dir_count_r"] or 0,
    "dirs_r": lambda d: d["dir_count_r"] or 0,
    "dirs_nr": lambda d: d["dir_count_nr"] or 0,
    "atime_r": lambda d: d["max_atime_r"] or "",
    "path": lambda d: (d["depth"], d["path"]),
    "depth": lambda d: d["depth"],
}

# Entity-summary sort fields accepted by owner_summary/group_summary.
_ENTITY_SORT_MAP = {
    "size": "size",
    "files": "files",
    "dirs": "dirs",
    "directories": "dirs",
}


def _collapse_prefixes(prefixes) -> list[str]:
    """Drop any prefix that is a descendant of another; dedupe + sort.

    Overlapping prefixes (e.g. ``/mmm`` and ``/mmm/parc``) otherwise match the
    same directories once per ancestor — producing duplicate result rows and
    redundant subtree computation. Keeping only the shallowest covering
    prefixes yields identical coverage with no duplication.
    """
    out: list[str] = []
    for p in sorted(set(prefixes)):
        if not any(p == anc or p.startswith(anc.rstrip("/") + "/") for anc in out):
            out.append(p)
    return out


def _is_collection_root(norm_prefix: str) -> bool:
    """True when a normalized prefix names a whole collection (single segment).

    ``normalize_path`` strips mount prefixes down to ``/<collection>[/...]``;
    a single remaining segment (``/mmm``) is the collection root. Filtering on
    it covers the entire collection, so the pre-computed / unfiltered query
    path returns the identical result far faster (20-150x in practice).
    """
    return bool(norm_prefix) and "/" not in norm_prefix.strip("/")


class FsScanQueries:
    """Machine-/filesystem-aware query API for filesystem scan databases.

    Parameters
    ----------
    filesystems:
        ``"all"`` (default) to query every available collection (resolved via
        :func:`get_all_filesystems`), a single filesystem name, or an explicit
        list of names.
    data_dir:
        Optional override for the directory containing the SQLite ``*.db``
        files (ignored for the PostgreSQL backend). Applied process-wide via
        :func:`set_data_dir`, matching the CLI ``--data-dir`` option.
    """

    def __init__(self, filesystems="all", data_dir=None):
        if data_dir is not None:
            set_data_dir(data_dir)

        if isinstance(filesystems, str):
            if filesystems.lower() == "all":
                self.filesystems = get_all_filesystems()
            else:
                self.filesystems = [filesystems]
        else:
            self.filesystems = list(filesystems)

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------
    @property
    def multi_db(self) -> bool:
        """True when more than one filesystem will be queried."""
        return len(self.filesystems) > 1

    def scan_dates(self, filesystems=None) -> list[datetime]:
        """Collect scan timestamps across the configured filesystems.

        ``filesystems`` restricts the lookup to a subset (used by the
        histogram methods after the whole-collection-root fast-path narrows
        the queried collections); defaults to all configured filesystems.
        """
        dates = []
        for fs in (filesystems if filesystems is not None else self.filesystems):
            session = get_session(fs)
            try:
                scan_date = get_scan_date(session)
                if scan_date:
                    dates.append(scan_date)
            finally:
                session.close()
        return dates

    def _resolve_scope(self, path_prefixes):
        """Normalize + collapse *path_prefixes* and apply the root fast path.

        Returns ``(filesystems, norm_prefixes)``:

          * ``filesystems`` — the collections to query. Narrowed to exactly
            the collections named by the prefixes when every (collapsed)
            prefix is a whole-collection root, so we don't scan collections
            the caller didn't ask for; otherwise the configured
            ``self.filesystems``.
          * ``norm_prefixes`` — the collapsed, mount-normalized prefixes, or
            ``None`` when every prefix is a whole-collection root. ``None``
            routes each query method to its pre-computed / unfiltered fast
            path (OwnerSummary / GroupSummary / DirectoryStats /
            AccessHistogram), returning the identical result far faster.

        A root prefix naming a collection that isn't configured falls back to
        the filtered path over ``self.filesystems`` (which simply matches
        nothing for that prefix) rather than silently widening scope.
        """
        if not path_prefixes:
            return self.filesystems, None

        norm = _collapse_prefixes(normalize_path(p) for p in path_prefixes)
        if norm and all(_is_collection_root(p) for p in norm):
            named = {p.strip("/").lower() for p in norm}
            targets = [f for f in self.filesystems if f.lower() in named]
            if targets:
                return targets, None
        return self.filesystems, norm

    def summary(self) -> list[dict]:
        """Per-filesystem summary statistics (rows tagged with ``filesystem``)."""
        rows = []
        for fs in self.filesystems:
            session = get_session(fs)
            try:
                stats = get_summary(session)
            finally:
                session.close()
            stats["filesystem"] = fs
            rows.append(stats)
        return rows

    def resolve_usernames(self, uids) -> dict[int, str]:
        """Resolve UIDs to usernames across the configured filesystems."""
        return resolve_usernames_across_databases(uids, self.filesystems)

    def resolve_groupnames(self, gids) -> dict[int, str]:
        """Resolve GIDs to groupnames across the configured filesystems."""
        return resolve_groupnames_across_databases(gids, self.filesystems)

    # ------------------------------------------------------------------
    # Directory listing
    # ------------------------------------------------------------------
    def list_directories(
        self,
        *,
        min_depth: int | None = None,
        max_depth: int | None = None,
        single_owner: bool = False,
        owner_id: int | None = None,
        path_prefixes=None,
        exclude_paths=None,
        sort_by: str = "size",
        limit: int | None = 50,
        accessed_before: datetime | None = None,
        accessed_after: datetime | None = None,
        leaves_only: bool = False,
        name_patterns=None,
        name_pattern_ignorecase: bool = False,
        min_size: int | None = None,
        max_size: int | None = None,
        min_files: int | None = None,
        max_files: int | None = None,
        compute_dir_counts: bool = False,
    ) -> list[dict]:
        """Return directory statistics across the configured filesystems.

        Path prefixes/excludes are normalized internally (mount-point
        prefixes stripped). For a single filesystem the query runs inline; for
        multiple filesystems each is queried in parallel and the combined
        result set is re-sorted and truncated to ``limit``.
        """
        filesystems, norm_prefixes = self._resolve_scope(path_prefixes)
        norm_excludes = [normalize_path(p) for p in exclude_paths] if exclude_paths else None
        query_limit = limit if (limit is not None and limit > 0) else None

        if len(filesystems) <= 1:
            if not filesystems:
                return []
            session = get_session(filesystems[0])
            try:
                return query_directories(
                    session,
                    min_depth=min_depth,
                    max_depth=max_depth,
                    single_owner=single_owner,
                    owner_id=owner_id,
                    path_prefixes=norm_prefixes,
                    exclude_paths=norm_excludes,
                    sort_by=sort_by,
                    limit=query_limit,
                    accessed_before=accessed_before,
                    accessed_after=accessed_after,
                    leaves_only=leaves_only,
                    name_patterns=list(name_patterns) if name_patterns else None,
                    name_pattern_ignorecase=name_pattern_ignorecase,
                    min_size=min_size,
                    max_size=max_size,
                    min_files=min_files,
                    max_files=max_files,
                    compute_dir_counts=compute_dir_counts,
                )
            finally:
                session.close()

        # Multi-filesystem: parallel fan-out, then combine + re-sort + re-limit.
        all_directories: list[dict] = []
        max_workers = min(len(filesystems), 8)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    query_single_filesystem,
                    fs,
                    min_depth,
                    max_depth,
                    single_owner,
                    owner_id,
                    norm_prefixes,
                    norm_excludes,
                    sort_by,
                    query_limit,
                    accessed_before,
                    accessed_after,
                    leaves_only,
                    list(name_patterns) if name_patterns else None,
                    name_pattern_ignorecase,
                    min_size,
                    max_size,
                    min_files,
                    max_files,
                    compute_dir_counts,
                ): fs
                for fs in filesystems
            }
            for future in as_completed(futures):
                all_directories.extend(future.result())

        reverse = sort_by not in ("path",)
        all_directories.sort(
            key=_DIR_SORT_KEYS.get(sort_by, _DIR_SORT_KEYS["size_r"]),
            reverse=reverse,
        )
        if query_limit is not None:
            all_directories = all_directories[:query_limit]
        return all_directories

    # ------------------------------------------------------------------
    # Owner / group summaries
    # ------------------------------------------------------------------
    def owner_summary(self, *, breakdown: bool = False, **filters) -> list[dict]:
        """Per-owner aggregated statistics across the configured filesystems.

        See :meth:`_entity_summary` for parameters. ``breakdown=True`` keeps a
        row per (owner, filesystem) for multi-filesystem queries; otherwise the
        rows are aggregated by owner UID.
        """
        return self._entity_summary("owner", breakdown=breakdown, **filters)

    def group_summary(self, *, breakdown: bool = False, **filters) -> list[dict]:
        """Per-group aggregated statistics across the configured filesystems."""
        return self._entity_summary("group", breakdown=breakdown, **filters)

    def _entity_summary(
        self,
        entity_type: str,
        *,
        breakdown: bool = False,
        min_depth: int | None = None,
        max_depth: int | None = None,
        path_prefixes=None,
        limit: int | None = None,
        sort_by: str = "size",
    ) -> list[dict]:
        if entity_type == "owner":
            id_field = "owner_uid"
            query_func = query_owner_summary
        elif entity_type == "group":
            id_field = "owner_gid"
            query_func = query_group_summary
        else:  # pragma: no cover - guarded by callers
            raise ValueError(f"Invalid entity_type: {entity_type}")

        filesystems, norm_prefixes = self._resolve_scope(path_prefixes)
        entity_sort_by = _ENTITY_SORT_MAP.get(sort_by, "size")
        query_limit = limit if (limit is not None and limit > 0) else None

        # Query each filesystem (tag each row with its source filesystem).
        all_results: list[dict] = []
        for fs in filesystems:
            session = get_session(fs)
            try:
                results = query_func(
                    session,
                    min_depth=min_depth,
                    max_depth=max_depth,
                    path_prefixes=norm_prefixes,
                    limit=query_limit,
                    sort_by=entity_sort_by,
                )
            finally:
                session.close()
            for result in results:
                result["filesystem"] = fs
            all_results.extend(results)

        if len(filesystems) <= 1:
            return all_results

        if breakdown:
            # Per-filesystem breakdown: sort the tagged rows, then truncate.
            sort_key_map = {
                "size": lambda r: (-r["total_size"], r[id_field], r["filesystem"]),
                "files": lambda r: (-r["total_files"], r[id_field], r["filesystem"]),
                "dirs": lambda r: (-r["directory_count"], r[id_field], r["filesystem"]),
            }
            all_results.sort(key=sort_key_map[entity_sort_by])
        else:
            # Aggregate across filesystems by entity id.
            aggregated = defaultdict(
                lambda: {"total_size": 0, "total_files": 0, "directory_count": 0}
            )
            for result in all_results:
                entity_id = result[id_field]
                aggregated[entity_id][id_field] = entity_id
                aggregated[entity_id]["total_size"] += result["total_size"]
                aggregated[entity_id]["total_files"] += result["total_files"]
                aggregated[entity_id]["directory_count"] += result["directory_count"]
            all_results = list(aggregated.values())

            agg_sort_key = {
                "size": lambda r: -r["total_size"],
                "files": lambda r: -r["total_files"],
                "dirs": lambda r: -r["directory_count"],
            }
            all_results.sort(key=agg_sort_key[entity_sort_by])

        if query_limit is not None:
            all_results = all_results[:query_limit]
        return all_results

    # ------------------------------------------------------------------
    # Histograms (analyze)
    # ------------------------------------------------------------------
    def access_history(
        self,
        *,
        owner_uid: int | None = None,
        path_prefixes=None,
        min_depth: int | None = None,
        max_depth: int | None = None,
    ) -> dict | None:
        """Access-time distribution histogram across the configured filesystems.

        Returns a serializable dict (see :meth:`_histogram_dict`) or ``None``
        when no scan dates are available. Uses the pre-computed ORM fast path
        when no path/depth filters are given, otherwise computes on the fly
        from ``directory_stats``.
        """
        filesystems, norm_prefixes = self._resolve_scope(path_prefixes)
        use_fast_path = norm_prefixes is None and not min_depth and not max_depth
        display_dir = self._display_directory(norm_prefixes, filesystems)

        scan_dates = self.scan_dates(filesystems)
        if not scan_dates:
            return None
        reference_scan_date = max(scan_dates)

        if use_fast_path:
            combined, username_map = aggregate_histograms_across_databases(
                filesystems=filesystems,
                histogram_type="access",
                owner_uid=owner_uid,
            )
            return self._histogram_dict(
                combined, username_map,
                histogram_type="access", renderer="histogram_data",
                title="Access Time Distribution", directory=display_dir,
                fast_path=True, approximate=False,
                scan_dates=scan_dates, reference_scan_date=reference_scan_date,
            )

        # Slow path: compute per-filesystem AccessHistogram and merge.
        combined = AccessHistogram(reference_scan_date)
        all_uids: set[int] = set()
        for fs in filesystems:
            session = get_session(fs)
            try:
                scan_date = get_scan_date(session)
                if not scan_date:
                    continue
                fs_hist = compute_access_history(
                    session, scan_date,
                    path_prefixes=norm_prefixes,
                    min_depth=min_depth, max_depth=max_depth,
                )
            finally:
                session.close()

            combined.total_data += fs_hist.total_data
            combined.total_files += fs_hist.total_files
            for label in combined.buckets:
                src = fs_hist.buckets[label]
                dst = combined.buckets[label]
                dst["data"] += src["data"]
                dst["files"] += src["files"]
                for uid, stats in src["owners"].items():
                    dst["owners"][uid]["data"] += stats["data"]
                    dst["owners"][uid]["files"] += stats["files"]
                    all_uids.add(uid)

        username_map = self.resolve_usernames(all_uids)
        return self._histogram_dict(
            combined, username_map,
            histogram_type="access", renderer="access_histogram",
            title=None, directory=display_dir,
            fast_path=False, approximate=False,
            scan_dates=scan_dates, reference_scan_date=reference_scan_date,
        )

    def file_size_histogram(
        self,
        *,
        owner_uid: int | None = None,
        path_prefixes=None,
        min_depth: int | None = None,
        max_depth: int | None = None,
    ) -> dict | None:
        """File-size distribution histogram across the configured filesystems.

        Returns a serializable dict or ``None`` when no scan dates are
        available. Uses the ORM fast path when unfiltered, otherwise an
        approximate computation from ``directory_stats``.
        """
        filesystems, norm_prefixes = self._resolve_scope(path_prefixes)
        use_fast_path = norm_prefixes is None and not min_depth and not max_depth
        display_dir = self._display_directory(norm_prefixes, filesystems)

        scan_dates = self.scan_dates(filesystems)
        if not scan_dates:
            return None
        reference_scan_date = max(scan_dates)

        if use_fast_path:
            combined, username_map = aggregate_histograms_across_databases(
                filesystems=filesystems,
                histogram_type="size",
                owner_uid=owner_uid,
            )
            return self._histogram_dict(
                combined, username_map,
                histogram_type="size", renderer="histogram_data",
                title="File Size Distribution", directory=display_dir,
                fast_path=True, approximate=False,
                scan_dates=scan_dates, reference_scan_date=reference_scan_date,
            )

        # Slow path: approximate per-filesystem size histogram and merge.
        bucket_labels = [label for label, _, _ in SIZE_BUCKETS]
        combined = HistogramData(bucket_labels, reference_scan_date)
        all_uids: set[int] = set()
        for fs in filesystems:
            session = get_session(fs)
            try:
                scan_date = get_scan_date(session)
                if not scan_date:
                    continue
                fs_hist = compute_size_histogram_from_directory_stats(
                    session, scan_date,
                    path_prefixes=norm_prefixes,
                    min_depth=min_depth, max_depth=max_depth,
                    owner_uid=owner_uid,
                )
            finally:
                session.close()

            for bucket_label, owner_data in fs_hist.items():
                for uid, (file_count, total_size) in owner_data.items():
                    combined.add_bucket_data(bucket_label, uid, file_count, total_size)
                    if uid is not None and uid >= 0:
                        all_uids.add(uid)

        username_map = self.resolve_usernames(all_uids)
        return self._histogram_dict(
            combined, username_map,
            histogram_type="size", renderer="histogram_data",
            title="File Size Distribution (Approximate)", directory=display_dir,
            fast_path=False, approximate=True,
            scan_dates=scan_dates, reference_scan_date=reference_scan_date,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _display_directory(self, norm_prefixes, filesystems=None) -> str:
        """Compute the human-readable 'Directory' label for histogram output.

        ``filesystems`` is the (possibly root-narrowed) set actually queried;
        defaults to all configured filesystems. When a whole-collection-root
        prefix collapses to ``norm_prefixes=None`` over a single collection,
        the label reflects that collection (``/mmm``) rather than "All
        filesystems".
        """
        fs = filesystems if filesystems is not None else self.filesystems
        if norm_prefixes:
            return norm_prefixes[0] if len(norm_prefixes) == 1 else "Multiple paths"
        if len(fs) == 1:
            return f"/{fs[0]}"
        return "All filesystems"

    @staticmethod
    def _histogram_dict(
        combined, username_map, *, histogram_type, renderer, title, directory,
        fast_path, approximate, scan_dates, reference_scan_date,
    ) -> dict:
        """Build the canonical histogram result dict from a combined histogram.

        Carries the bucket data plus the render metadata needed to reproduce
        the exact CLI text output (``renderer``/``title``) without re-running
        the query.
        """
        data = combined.to_dict()
        data.update(
            histogram_type=histogram_type,
            renderer=renderer,
            title=title,
            directory=directory,
            fast_path=fast_path,
            approximate=approximate,
            scan_dates=scan_dates,
            reference_scan_date=reference_scan_date,
            username_map=username_map,
        )
        return data
