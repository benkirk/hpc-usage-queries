"""Tests for filesystem scan parsers."""

import pytest
from pathlib import Path
from datetime import datetime

from fs_scans.parsers.base import FilesystemParser, ParsedEntry
from fs_scans.parsers.gpfs import GPFSParser
from fs_scans.parsers import get_parser, detect_parser, list_formats


# ============================================================================
# GPFS Parser Tests
# ============================================================================


class TestGPFSParser:
    """Tests for GPFS parser."""

    def test_format_name(self):
        """Test parser format name."""
        parser = GPFSParser()
        assert parser.format_name == "gpfs"

    def test_can_parse_valid_filename(self):
        """Test auto-detection of GPFS format by filename."""
        parser = GPFSParser()

        # Valid GPFS filenames
        assert parser.can_parse(Path("20260111_csfs1_asp.list.list_all.log"))
        assert parser.can_parse(Path("20240101_filesystem_fileset.list"))
        assert parser.can_parse(Path("20231225_test_data.list.gz"))

    def test_can_parse_invalid_filename(self):
        """Test rejection of non-GPFS filenames."""
        parser = GPFSParser()

        # Invalid filenames
        assert not parser.can_parse(Path("scan.log"))
        assert not parser.can_parse(Path("data.csv"))
        assert not parser.can_parse(Path("filesystem_scan.txt"))

    def test_parse_file_line(self):
        """Test parsing a file entry."""
        parser = GPFSParser()
        line = "<0> 123456 1 0 s=4096 a=4 u=1000 g=100 p=-rw-r--r-- ac=2024-01-15 10:30:00 -- /path/to/file.txt"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.path == "/path/to/file.txt"
        assert entry.size == 4096
        assert entry.allocated == 4096  # 4 KB * 1024
        assert entry.uid == 1000
        assert entry.gid == 100
        assert entry.is_dir is False
        assert entry.atime == datetime(2024, 1, 15, 10, 30, 0)
        assert entry.inode == 123456
        assert entry.fileset_id == 1

    def test_parse_directory_line(self):
        """Test parsing a directory entry."""
        parser = GPFSParser()
        line = "<0> 789012 2 0 s=4096 a=4 u=2000 g=200 p=drwxr-xr-x ac=2024-01-15 12:00:00 -- /path/to/dir"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.path == "/path/to/dir"
        assert entry.is_dir is True
        assert entry.uid == 2000
        assert entry.gid == 200

    def test_parse_line_gpfs_inode_quirk(self):
        """Test GPFS quirk where small files have allocated=0."""
        parser = GPFSParser()
        # Small file with allocated=0 (stored in inode)
        line = "<0> 123456 1 0 s=512 a=0 u=1000 g=100 p=-rw-r--r-- ac=2024-01-15 10:30:00 -- /small.txt"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.size == 512
        assert entry.allocated == 512  # Should be set to size for small files

    def test_parse_line_no_atime(self):
        """Test parsing line without access time."""
        parser = GPFSParser()
        line = "<0> 123456 1 0 s=1024 a=1 u=1000 g=100 p=-rw-r--r-- -- /path/to/file.txt"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.atime is None

    def test_parse_line_invalid(self):
        """Test parsing invalid line returns None."""
        parser = GPFSParser()

        # Not a GPFS format line
        assert parser.parse_line("invalid line") is None
        assert parser.parse_line("") is None
        assert parser.parse_line("# comment") is None

    def test_parse_line_missing_required_fields(self):
        """Test parsing line with missing required fields returns None."""
        parser = GPFSParser()

        # Missing size field
        line = "<0> 123456 1 0 a=4 u=1000 g=100 p=-rw-r--r-- -- /file.txt"
        assert parser.parse_line(line) is None

        # Missing user field
        line = "<0> 123456 1 0 s=4096 a=4 g=100 p=-rw-r--r-- -- /file.txt"
        assert parser.parse_line(line) is None

        # Missing group field
        line = "<0> 123456 1 0 s=4096 a=4 u=1000 p=-rw-r--r-- -- /file.txt"
        assert parser.parse_line(line) is None


# ============================================================================
# Parser Registry Tests
# ============================================================================


class TestParserRegistry:
    """Tests for parser registration and discovery."""

    def test_list_formats(self):
        """Test listing registered formats."""
        formats = list_formats()
        assert "gpfs" in formats
        assert isinstance(formats, list)

    def test_get_parser_by_name(self):
        """Test retrieving parser by name."""
        parser = get_parser("gpfs")
        assert isinstance(parser, GPFSParser)
        assert parser.format_name == "gpfs"

    def test_get_parser_invalid_name(self):
        """Test error when requesting non-existent parser."""
        with pytest.raises(ValueError, match="Unknown format"):
            get_parser("nonexistent")

    def test_detect_parser_gpfs(self):
        """Test auto-detection for GPFS files."""
        parser = detect_parser(Path("20260111_csfs1_asp.list.list_all.log"))
        assert parser is not None
        assert isinstance(parser, GPFSParser)

    def test_detect_parser_unknown_returns_none(self):
        """Test unknown format returns None."""
        parser = detect_parser(Path("unknown_format.txt"))
        assert parser is None


# ============================================================================
# ParsedEntry Tests
# ============================================================================


class TestParsedEntry:
    """Tests for ParsedEntry dataclass."""

    def test_create_full_entry(self):
        """Test creating entry with all fields."""
        entry = ParsedEntry(
            path="/test/path",
            size=1024,
            allocated=2048,
            uid=1000,
            gid=100,
            is_dir=False,
            atime=datetime(2024, 1, 15, 10, 30, 0),
            inode=123456,
            fileset_id=1,
        )

        assert entry.path == "/test/path"
        assert entry.size == 1024
        assert entry.allocated == 2048
        assert entry.uid == 1000
        assert entry.gid == 100
        assert entry.is_dir is False
        assert entry.atime == datetime(2024, 1, 15, 10, 30, 0)
        assert entry.inode == 123456
        assert entry.fileset_id == 1

    def test_create_minimal_entry(self):
        """Test creating entry with optional fields as None."""
        entry = ParsedEntry(
            path="/test/path",
            size=1024,
            allocated=2048,
            uid=1000,
            gid=100,
            is_dir=True,
            atime=None,
        )

        assert entry.atime is None
        assert entry.inode is None
        assert entry.fileset_id is None


# ============================================================================
# Placeholder Parser Tests
# ============================================================================


class TestPlaceholderParsers:
    """Tests for placeholder parsers (Lustre, POSIX)."""

    def test_lustre_parser_not_implemented(self):
        """Test that Lustre parser raises NotImplementedError."""
        from fs_scans.parsers.lustre import LustreParser

        parser = LustreParser()
        assert parser.format_name == "lustre"

        with pytest.raises(NotImplementedError):
            parser.can_parse(Path("scan.log"))

        with pytest.raises(NotImplementedError):
            parser.parse_line("test line")

    def test_posix_parser_not_implemented(self):
        """Test that POSIX parser raises NotImplementedError."""
        from fs_scans.parsers.posix import POSIXParser

        parser = POSIXParser()
        assert parser.format_name == "posix"

        with pytest.raises(NotImplementedError):
            parser.can_parse(Path("scan.log"))

        with pytest.raises(NotImplementedError):
            parser.parse_line("test line")
