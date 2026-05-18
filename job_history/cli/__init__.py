"""SAM-aligned CLI package for job_history.

Package layout mirrors project_samuel's src/cli/:

    cli/
      core/         shared infrastructure (Context, BaseCommand, exporters, exit codes)
      history/      history subcommands (jobs-per-user, daily-summary, ...)
      resource/     resource subcommands (driven by RESOURCE_REPORTS + BaseResourceCommand)
      sync/         sync subcommand
      cmds/         Click entry points (jobhist, jobhist-history, jobhist-resource, jobhist-sync)

The top-level ``cli`` Click group is re-exported here so the legacy
``from job_history.cli import cli`` import path keeps working — this is
what older installed scripts ``pyproject.toml`` may resolve.
"""

from job_history.cli.cmds.jobhist import cli

__all__ = ["cli"]
