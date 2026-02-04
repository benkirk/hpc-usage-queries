"""Parser registry and management for filesystem scan formats."""

from pathlib import Path
from typing import Type

from .base import FilesystemParser, ParsedEntry

# Registry of available parsers: format_name -> parser_class
_PARSER_REGISTRY: dict[str, Type[FilesystemParser]] = {}


def register_parser(parser_class: Type[FilesystemParser]) -> None:
    """Register a parser class in the global registry.

    Args:
        parser_class: Parser class (not instance) to register

    Example:
        >>> register_parser(GPFSParser)
    """
    instance = parser_class()
    format_name = instance.format_name
    if format_name in _PARSER_REGISTRY:
        raise ValueError(f"Parser for format '{format_name}' already registered")
    _PARSER_REGISTRY[format_name] = parser_class


def get_parser(format_name: str) -> FilesystemParser:
    """Get parser instance by format name.

    Args:
        format_name: Format identifier (e.g., 'gpfs', 'lustre', 'posix')

    Returns:
        New instance of the requested parser

    Raises:
        ValueError: If format_name is not registered

    Example:
        >>> parser = get_parser("gpfs")
    """
    if format_name not in _PARSER_REGISTRY:
        available = ", ".join(sorted(_PARSER_REGISTRY.keys()))
        raise ValueError(
            f"Unknown format: '{format_name}'. Available formats: {available}"
        )
    return _PARSER_REGISTRY[format_name]()


def detect_parser(file_path: Path) -> FilesystemParser | None:
    """Auto-detect parser for a file.

    Tries each registered parser's can_parse() method in registration order.
    Returns the first parser that claims it can handle the file.

    Args:
        file_path: Path to scan log file

    Returns:
        Parser instance if format detected, None if no parser matches

    Example:
        >>> parser = detect_parser(Path("scan.log"))
        >>> if parser:
        ...     print(f"Detected format: {parser.format_name}")
    """
    # Convert to Path if string was passed
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    for parser_class in _PARSER_REGISTRY.values():
        parser = parser_class()
        if parser.can_parse(file_path):
            return parser
    return None


def list_formats() -> list[str]:
    """List all registered format names.

    Returns:
        Sorted list of format identifiers
    """
    return sorted(_PARSER_REGISTRY.keys())


# Import and auto-register parsers
# (Import order determines detection priority if multiple parsers match)
from .gpfs import GPFSParser

register_parser(GPFSParser)

# Future parsers (uncomment when implemented):
from .lustre import LustreParser
register_parser(LustreParser)
# from .posix import POSIXParser
# register_parser(POSIXParser)


# Export public API
__all__ = [
    "FilesystemParser",
    "ParsedEntry",
    "register_parser",
    "get_parser",
    "detect_parser",
    "list_formats",
]
