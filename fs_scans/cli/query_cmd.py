"""Query subcommand for fs-scans CLI.

TEMPORARY: This currently wraps the existing query_db.py implementation.
TODO: Refactor query_db.py to separate CLI from business logic:
  - Move query logic to queries/query_engine.py
  - Keep only CLI interface here
"""

# For now, import and re-export the main function from query_db
from ..query_db import main as query_cmd

# The query_cmd Click command is now available for registration in main.py
# It already has all the Click decorators and options from query_db.py
