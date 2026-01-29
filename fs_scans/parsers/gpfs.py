"""GPFS filesystem scan log parser."""

import re
from datetime import datetime
from pathlib import Path

from .base import FilesystemParser, ParsedEntry


# Extended LINE_PATTERN that captures inode and fileset_id for unique identification
# Format: <thread> inode fileset_id snapshot  fields -- /path
LINE_PATTERN = re.compile(
    r"^<\d+>\s+(\d+)\s+(\d+)\s+\d+\s+"  # <thread> inode fileset_id snapshot
    r"(.+?)\s+--\s+(.+)$"  # fields -- path
)

# Pattern to extract specific fields from the key=value section of GPFS scan lines
FIELD_PATTERNS = {
    "size": re.compile(r"s=(\d+)"),
    "allocated_kb": re.compile(r"a=(\d+)"),
    "user_id": re.compile(r"u=(\d+)"),
    "permissions": re.compile(r"p=([^\s]+)"),
    "atime": re.compile(r"ac=(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"),
}


class GPFSParser(FilesystemParser):
    """Parser for GPFS policy scan log files.

    GPFS scan logs have the format:
    <thread> inode fileset_id snapshot key=value pairs -- /path

    Example line:
    <0> 123456 1 0 s=4096 a=4 u=1000 p=drwxr-xr-x ac=2024-01-15 10:30:00 -- /path/to/dir
    """

    @property
    def format_name(self) -> str:
        """Return parser format identifier."""
        return "gpfs"

    def can_parse(self, file_path: Path) -> bool:
        """Auto-detect if this parser can handle the file.

        Detects GPFS format by filename pattern: YYYYMMDD_filesystem_fileset.list*
        """
        # Check filename pattern: YYYYMMDD_*_*.list*
        pattern = r"^\d{8}_[^_]+_[^.]+\.list"
        return bool(re.match(pattern, file_path.name))

    def parse_line(self, line: str) -> ParsedEntry | None:
        """Parse a single GPFS scan log line.

        Args:
            line: A single line from the GPFS scan log

        Returns:
            ParsedEntry if the line was successfully parsed, None to skip
        """
        match = LINE_PATTERN.match(line)
        if not match:
            return None

        inode, fileset_id, fields_str, path = match.groups()

        # Extract permissions to check if file or directory
        perm_match = FIELD_PATTERNS["permissions"].search(fields_str)
        if not perm_match:
            return None

        permissions = perm_match.group(1)
        is_dir = permissions.startswith("d")

        # Extract other fields
        size_match = FIELD_PATTERNS["size"].search(fields_str)
        alloc_match = FIELD_PATTERNS["allocated_kb"].search(fields_str)
        user_match = FIELD_PATTERNS["user_id"].search(fields_str)
        atime_match = FIELD_PATTERNS["atime"].search(fields_str)

        if not all([size_match, user_match]):
            return None

        # Parse atime
        atime = None
        if atime_match:
            try:
                atime = datetime.strptime(atime_match.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        # Size is in bytes
        size = int(size_match.group(1))

        # Allocated is in KB, convert to bytes
        allocated = int(alloc_match.group(1)) * 1024 if alloc_match else 0

        # GPFS weirdness: data can be stored in the inode when the size is small.
        # If allocated is 0 but file size is small, assume it's stored in the inode.
        if allocated == 0:
            if size <= 4096:
                allocated = size

        return ParsedEntry(
            path=path,
            size=size,
            allocated=allocated,
            uid=int(user_match.group(1)),
            is_dir=is_dir,
            atime=atime,
            inode=int(inode),
            fileset_id=int(fileset_id),
        )
