"""Tests for filesystem scan parsers."""

import pytest
from pathlib import Path
from datetime import datetime

from fs_scans.parsers.base import FilesystemParser, ParsedEntry
from fs_scans.parsers.gpfs import GPFSParser
from fs_scans.parsers.lustre import LustreParser
from fs_scans.parsers import get_parser, detect_parser, list_formats
from fs_scans.core.database import extract_filesystem_from_filename


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
        assert "lustre" in formats
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

    def test_detect_parser_lustre(self):
        """Test auto-detection for Lustre files."""
        parser = detect_parser(Path("20260204_desc1_gdex.lfs-scan"))
        assert parser is not None
        assert isinstance(parser, LustreParser)

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
# Lustre Parser Tests
# ============================================================================


class TestLustreParser:
    """Tests for Lustre parser."""

    def test_format_name(self):
        """Test parser format name."""
        parser = LustreParser()
        assert parser.format_name == "lustre"

    def test_can_parse_valid_filename(self):
        """Test auto-detection of Lustre format by filename."""
        parser = LustreParser()

        # Valid Lustre filenames
        assert parser.can_parse(Path("20260204_desc1_gdex.lfs-scan"))
        assert parser.can_parse(Path("20260204_desc1_glade_p_archive.lfs-scan"))
        assert parser.can_parse(Path("scan_output.lfs-scan"))

    def test_can_parse_invalid_filename(self):
        """Test rejection of non-Lustre filenames."""
        parser = LustreParser()

        # Invalid filenames
        assert not parser.can_parse(Path("scan.log"))
        assert not parser.can_parse(Path("data.csv"))
        assert not parser.can_parse(Path("20260204_desc1_gdex.list"))

    def test_parse_file_line(self):
        """Test parsing a file entry."""
        parser = LustreParser()
        line = "0x28001ff98:0xe66b:0x0 s=51 b=8 u=16093 g=4801 p=1 type=f perm=0755 a=1708547123 m=1670625642 c=1708547123 -- /lustre/path/file.txt"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.path == "/lustre/path/file.txt"
        assert entry.size == 51
        assert entry.allocated == 4096  # 8 blocks * 512
        assert entry.uid == 16093
        assert entry.gid == 4801
        assert entry.is_dir is False
        assert entry.atime == datetime.fromtimestamp(1708547123)
        assert entry.inode is None
        assert entry.fileset_id is None

    def test_parse_directory_line(self):
        """Test parsing a directory entry."""
        parser = LustreParser()
        line = "0x24001959d:0x1f:0x0 s=16384 b=32 u=38057 g=68122 p=1 type=d perm=0750 a=1769700762 m=1739055225 c=1739055225 -- /lustre/path/dir"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.path == "/lustre/path/dir"
        assert entry.size == 16384
        assert entry.allocated == 16384  # 32 blocks * 512
        assert entry.is_dir is True
        assert entry.uid == 38057
        assert entry.gid == 68122

    def test_parse_large_file(self):
        """Test parsing a large file with correct block calculation."""
        parser = LustreParser()
        # Large file: 159GB with 311033880 blocks
        line = "0x28001ff98:0xe66c:0x0 s=159249182720 b=311033880 u=16093 g=4801 p=1 type=f perm=0755 a=1708547467 m=1539293571 c=1708547467 -- /lustre/data.tar"

        entry = parser.parse_line(line)

        assert entry is not None
        assert entry.size == 159249182720
        assert entry.allocated == 159249346560  # 311033880 * 512
        assert entry.is_dir is False

    def test_parse_line_invalid(self):
        """Test parsing invalid line returns None."""
        parser = LustreParser()

        # Invalid lines
        assert parser.parse_line("invalid line") is None
        assert parser.parse_line("") is None
        assert parser.parse_line("# comment") is None

    def test_parse_line_missing_required_fields(self):
        """Test parsing line with missing required fields returns None."""
        parser = LustreParser()

        # Missing size field
        line = "0x28001ff98:0xe66b:0x0 b=8 u=16093 g=4801 p=1 type=f perm=0755 a=1708547123 -- /file.txt"
        assert parser.parse_line(line) is None

        # Missing blocks field
        line = "0x28001ff98:0xe66b:0x0 s=51 u=16093 g=4801 p=1 type=f perm=0755 a=1708547123 -- /file.txt"
        assert parser.parse_line(line) is None

        # Missing type field
        line = "0x28001ff98:0xe66b:0x0 s=51 b=8 u=16093 g=4801 p=1 perm=0755 a=1708547123 -- /file.txt"
        assert parser.parse_line(line) is None


# ============================================================================
# Filesystem Name Extraction Tests
# ============================================================================


class TestFilesystemNameExtraction:
    """Tests for filesystem name extraction from filenames."""

    def test_extract_gpfs_filename(self):
        """Test extracting filesystem from GPFS filename."""
        assert extract_filesystem_from_filename("20260111_csfs1_asp.list.list_all.log") == "asp"
        assert extract_filesystem_from_filename("20260111_csfs1_cisl.list") == "cisl"
        assert extract_filesystem_from_filename("20240101_server_filesystem.list.gz") == "filesystem"

    def test_extract_lustre_filename(self):
        """Test extracting filesystem from Lustre filename."""
        assert extract_filesystem_from_filename("20260204_desc1_gdex.lfs-scan") == "gdex"
        assert extract_filesystem_from_filename("20260204_desc1_glade_p_archive.lfs-scan") == "glade_p_archive"

    def test_extract_invalid_filename(self):
        """Test that invalid filenames return None."""
        assert extract_filesystem_from_filename("scan.log") is None
        assert extract_filesystem_from_filename("data.csv") is None
        assert extract_filesystem_from_filename("random_file.txt") is None


# ============================================================================
# Placeholder Parser Tests
# ============================================================================


class TestPlaceholderParsers:
    """Tests for placeholder parsers (POSIX)."""

    def test_posix_parser_not_implemented(self):
        """Test that POSIX parser raises NotImplementedError."""
        from fs_scans.parsers.posix import POSIXParser

        parser = POSIXParser()
        assert parser.format_name == "posix"

        with pytest.raises(NotImplementedError):
            parser.can_parse(Path("scan.log"))

        with pytest.raises(NotImplementedError):
            parser.parse_line("test line")
