"""Rich rendering for history envelopes.

Used by the generic :class:`RichTableExporter` for the column-driven
envelopes built in :mod:`job_history.cli.history.builders`.

Display functions here are dict-only — they never touch ORM objects.
For now the generic exporter handles every history envelope correctly;
this module exists so future bespoke renderers (titled panels, nested
tables, etc.) have a clear home.
"""
