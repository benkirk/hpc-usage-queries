"""Lustre filesystem scan log parser."""

import re
from datetime import datetime
from pathlib import Path

from .base import FilesystemParser, ParsedEntry

# Regex pattern for matching overall line structure
# Format: <FID> <fields> -- <path>
# Example: 0x24001959d:0x1f:0x0 s=16384 b=32 u=38057 g=68122 p=1 type=d perm=0750 a=1769700762 m=1739055225 c=1739055225 -- /path
LINE_PATTERN = re.compile(
    r"^0x[0-9a-f]+:0x[0-9a-f]+:0x[0-9a-f]+\s+"  # FID (hex triplet)
    r"(.+?)\s+--\s+"  # fields
    r"(.+)$"  # path
)

# Regex patterns for extracting individual fields
FIELD_PATTERNS = {
    "size": re.compile(r"s=(\d+)"),
    "blocks": re.compile(r"b=(\d+)"),
    "user_id": re.compile(r"u=(\d+)"),
    "group_id": re.compile(r"g=(\d+)"),
    "file_type": re.compile(r"type=([df])"),
    "atime": re.compile(r"a=(\d+)"),
}


class LustreParser(FilesystemParser):
    """Parser for Lustre filesystem scan logs.

    Lustre scan logs are generated using `lfs find` with custom output formats.
    See bin/lustre_scan.sh for the scan script that generates these files.

    Expected format (from lfs find --printf):
    FID s=<size> b=<blocks> u=<uid> g=<gid> p=<val> type=<d|f> perm=<perm> a=<atime> m=<mtime> c=<ctime> -- <path>

    Where:
    - FID: Lustre File Identifier (0xHEX:0xHEX:0xHEX) - ignored
    - s=<size>: File size in bytes
    - b=<blocks>: Number of 512-byte blocks allocated
    - u=<uid>: User ID
    - g=<gid>: Group ID
    - type=<d|f>: 'd' for directory, 'f' for file
    - a=<atime>: Access time (Unix timestamp)
    - path: Full filesystem path
    """

    @property
    def format_name(self) -> str:
        """Return parser format identifier."""
        return "lustre"

    def can_parse(self, file_path: Path) -> bool:
        """Auto-detect if this parser can handle the file.

        Detects Lustre format by filename pattern: *.lfs-scan
        """
        return file_path.name.endswith(".lfs-scan")

    def parse_line(self, line: str) -> ParsedEntry | None:
        """Parse a single Lustre scan log line.

        Extracts fields and returns a normalized ParsedEntry. Returns None
        for invalid lines (comments, headers, malformed data).

        Args:
            line: A single line from the Lustre scan log

        Returns:
            ParsedEntry if the line was successfully parsed, None to skip
        """
        # Match overall line structure
        match = LINE_PATTERN.match(line)
        if not match:
            return None

        fields_str, path = match.groups()

        # Extract required fields
        size_match = FIELD_PATTERNS["size"].search(fields_str)
        blocks_match = FIELD_PATTERNS["blocks"].search(fields_str)
        user_match = FIELD_PATTERNS["user_id"].search(fields_str)
        group_match = FIELD_PATTERNS["group_id"].search(fields_str)
        type_match = FIELD_PATTERNS["file_type"].search(fields_str)
        atime_match = FIELD_PATTERNS["atime"].search(fields_str)

        # Validate all required fields are present
        if not all([size_match, blocks_match, user_match, group_match, type_match, atime_match]):
            return None

        # Extract and convert values
        size = int(size_match.group(1))
        blocks = int(blocks_match.group(1))
        allocated = blocks * 512  # Convert blocks to bytes
        uid = int(user_match.group(1))
        gid = int(group_match.group(1))
        is_dir = type_match.group(1) == "d"
        atime_timestamp = int(atime_match.group(1))
        atime = datetime.fromtimestamp(atime_timestamp)  # Timezone-naive for compatibility

        return ParsedEntry(
            path=path,
            size=size,
            allocated=allocated,
            uid=uid,
            gid=gid,
            is_dir=is_dir,
            atime=atime,
            inode=None,  # Lustre FID not stored as inode
            fileset_id=None,  # Not applicable for Lustre
        )
