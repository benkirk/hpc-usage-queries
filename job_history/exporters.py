"""Export formatters for qhist-queries reports.

This module provides different export formats for job history reports,
allowing data to be output in DAT, JSON, CSV, or Markdown formats.
"""

import json
import csv
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import date


class ReportExporter(ABC):
    """Abstract base class for report exporters."""

    @abstractmethod
    def export(self, data: List[Dict], columns: List[Any], filepath: str):
        """Export data in specific format.

        Args:
            data: List of dictionaries containing report data
            columns: List of ColumnSpec objects defining the output structure
            filepath: Path where the file should be written
        """
        pass


class DatExporter(ReportExporter):
    """Fixed-width .dat format exporter (traditional format)."""

    def export(self, data: List[Dict], columns: List[Any], filepath: str):
        """Export data as fixed-width DAT format.

        Args:
            data: List of dictionaries containing report data
            columns: List of ColumnSpec objects
            filepath: Output file path
        """
        with open(filepath, 'w') as f:
            # Write header
            header_parts = []
            for col in columns:
                if col.width > 0:
                    header_parts.append(f"{col.header:<{col.width}}")
                else:
                    header_parts.append(col.header)
            f.write("".join(header_parts) + "\n")

            # Write data rows
            for row in data:
                row_parts = []
                for col in columns:
                    value = row[col.key]
                    if col.width > 0:
                        if col.format == "s":
                            row_parts.append(f"{value:<{col.width}}")
                        elif col.format:
                            row_parts.append(f"{value:<{col.width}{col.format}}")
                        else:
                            row_parts.append(f"{value:<{col.width}}")
                    else:
                        # Last column
                        if col.format:
                            row_parts.append(f"{value:{col.format}}")
                        else:
                            row_parts.append(str(value))
                f.write("".join(row_parts) + "\n")


class JSONExporter(ReportExporter):
    """JSON format exporter for programmatic access."""

    def export(self, data: List[Dict], columns: List[Any], filepath: str):
        """Export data as JSON format.

        Args:
            data: List of dictionaries containing report data
            columns: List of ColumnSpec objects (not used for JSON, data already has keys)
            filepath: Output file path (.json extension recommended)
        """
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=self._json_serializer)

    @staticmethod
    def _json_serializer(obj):
        """Handle non-JSON-serializable types."""
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")


class CSVExporter(ReportExporter):
    """CSV format exporter for spreadsheet import."""

    def export(self, data: List[Dict], columns: List[Any], filepath: str):
        """Export data as CSV format.

        Args:
            data: List of dictionaries containing report data
            columns: List of ColumnSpec objects
            filepath: Output file path (.csv extension recommended)
        """
        if not data:
            # Write empty CSV with headers
            with open(filepath, 'w', newline='') as f:
                fieldnames = [col.key for col in columns]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
            return

        with open(filepath, 'w', newline='') as f:
            fieldnames = [col.key for col in columns]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)


class MarkdownExporter(ReportExporter):
    """Markdown table format exporter for documentation."""

    def export(self, data: List[Dict], columns: List[Any], filepath: str):
        """Export data as Markdown table format.

        Args:
            data: List of dictionaries containing report data
            columns: List of ColumnSpec objects
            filepath: Output file path (.md extension recommended)
        """
        with open(filepath, 'w') as f:
            # Write header row
            headers = [col.header for col in columns]
            f.write("| " + " | ".join(headers) + " |\n")

            # Write separator row
            separators = ["---" for _ in columns]
            f.write("| " + " | ".join(separators) + " |\n")

            # Write data rows
            for row in data:
                values = []
                for col in columns:
                    value = row[col.key]
                    # Format the value
                    if col.format and col.format not in ["s", ""]:
                        # Numeric format
                        formatted = f"{value:{col.format}}"
                    else:
                        formatted = str(value)
                    values.append(formatted)
                f.write("| " + " | ".join(values) + " |\n")


def get_exporter(format_type: str) -> ReportExporter:
    """Factory function to get the appropriate exporter.

    Args:
        format_type: Export format ('dat', 'json', 'csv', 'md')

    Returns:
        Instance of appropriate ReportExporter subclass

    Raises:
        ValueError: If format_type is not supported
    """
    exporters = {
        'dat': DatExporter,
        'json': JSONExporter,
        'csv': CSVExporter,
        'md': MarkdownExporter,
    }

    if format_type not in exporters:
        raise ValueError(f"Unsupported format: {format_type}. "
                        f"Supported formats: {', '.join(exporters.keys())}")

    return exporters[format_type]()
