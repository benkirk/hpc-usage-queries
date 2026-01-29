"""POSIX filesystem scan log parser (placeholder)."""

from pathlib import Path

from .base import FilesystemParser, ParsedEntry


class POSIXParser(FilesystemParser):
    """Parser for generic POSIX filesystem scan logs (not yet implemented).

    This parser is intended for general POSIX filesystems (ext4, XFS, ZFS, etc.)
    that don't have specialized scan tools. Scan logs can be generated using
    standard UNIX utilities:
    - find with -printf for formatted output
    - Custom scripts using stat
    - Tree-walk tools with JSON/CSV output

    Expected format (example - actual format TBD):
    JSON lines format:
    {"path": "/path", "size": 1024, "blocks": 8, "uid": 1000, "gid": 100,
     "mode": "0644", "atime": "2024-01-15T10:30:00", "is_dir": false}

    Alternative CSV format:
    path,size,blocks,uid,gid,mode,atime,mtime,is_dir
    /path,1024,8,1000,100,0644,2024-01-15T10:30:00,2024-01-15T10:30:00,0

    TODO: Define the exact format and implement parsing logic.
    """

    @property
    def format_name(self) -> str:
        """Return parser format identifier."""
        return "posix"

    def can_parse(self, file_path: Path) -> bool:
        """Auto-detect if this parser can handle the file.

        TODO: Implement detection logic based on:
        - Filename patterns (e.g., *.posix.scan, filesystem_scan.json, scan.csv)
        - File header/magic bytes (JSON array start, CSV header row)
        - Content inspection (first line matches expected format)
        """
        raise NotImplementedError(
            "POSIX parser not yet implemented. "
            "Detection logic needs to be defined based on scan tool output format."
        )

    def parse_line(self, line: str) -> ParsedEntry | None:
        """Parse a single POSIX scan log line.

        TODO: Implement parsing logic to extract:
        - path: Full filesystem path
        - size: Logical file size in bytes (from st_size)
        - allocated: Actual disk usage in bytes (st_blocks * 512)
        - uid: User ID (from st_uid)
        - is_dir: True for directories (S_ISDIR(st_mode))
        - atime: Last access time (from st_atime)

        Args:
            line: A single line from the POSIX scan log

        Returns:
            ParsedEntry if the line was successfully parsed, None to skip

        Raises:
            NotImplementedError: This parser is not yet implemented
        """
        raise NotImplementedError(
            "POSIX parser not yet implemented. "
            "Define scan log format (JSON, CSV, or custom) and implement field extraction."
        )
