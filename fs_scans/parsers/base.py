"""Base classes for filesystem scan log parsers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, TextIO


@dataclass
class ParsedEntry:
    """Normalized entry from any filesystem scan format.

    This dataclass represents a single file or directory entry parsed from
    a filesystem scan log. Different parsers will extract this information
    from different log formats (GPFS, Lustre, POSIX, etc.).
    """

    path: str
    """Full path to the file or directory."""

    size: int
    """Logical file size in bytes."""

    allocated: int
    """Actual disk space allocated in bytes (may differ from size due to compression, sparse files, etc.)."""

    uid: int
    """User ID (numeric UID) of the file owner."""

    gid: int
    """Group ID (numeric GID) of the file owner."""

    is_dir: bool
    """True if this entry represents a directory, False for files."""

    atime: datetime | None
    """Last access time, or None if not available."""

    inode: int | None = None
    """Inode number, if available from the scan format."""

    fileset_id: int | None = None
    """Fileset ID (GPFS-specific), if available."""


class FilesystemParser(ABC):
    """Abstract base class for filesystem scan log parsers.

    This class defines the interface that all filesystem parsers must implement.
    Parsers convert filesystem-specific scan log formats into normalized ParsedEntry objects.

    To implement a new parser:
    1. Subclass FilesystemParser
    2. Implement format_name property (returns unique format identifier)
    3. Implement can_parse() method (auto-detection logic)
    4. Implement parse_line() method (parse a single log line)
    5. Optionally override parse_file() for non-line-based formats
    6. Register the parser with register_parser() in parsers/__init__.py
    """

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Return parser format identifier (e.g., 'gpfs', 'lustre', 'posix').

        This should be a short, lowercase string that uniquely identifies the format.
        It's used for explicit format selection via --format flag.
        """
        pass

    @abstractmethod
    def can_parse(self, file_path: Path) -> bool:
        """Auto-detect if this parser can handle the given file.

        This method is used for automatic format detection. Implementations should
        check filename patterns, file extensions, or (optionally) peek at file contents
        to determine if they can parse the file.

        Args:
            file_path: Path to the scan log file

        Returns:
            True if this parser can handle the file, False otherwise
        """
        pass

    @abstractmethod
    def parse_line(self, line: str) -> ParsedEntry | None:
        """Parse a single line from the scan log.

        This is the core parsing method. It should extract fields from a single
        log line and return a ParsedEntry, or None if the line should be skipped
        (e.g., comments, headers, malformed lines).

        Args:
            line: A single line from the log file (newline stripped)

        Returns:
            ParsedEntry if the line was successfully parsed, None to skip
        """
        pass

    def parse_file(self, file_handle: TextIO) -> Iterator[ParsedEntry]:
        """Parse entire file line-by-line (default implementation).

        This default implementation calls parse_line() for each line in the file.
        It can be overridden for formats that require multi-line parsing or
        non-sequential access.

        Args:
            file_handle: Open file handle to read from

        Yields:
            ParsedEntry objects for each successfully parsed entry
        """
        for line in file_handle:
            entry = self.parse_line(line.rstrip('\n'))
            if entry:
                yield entry
