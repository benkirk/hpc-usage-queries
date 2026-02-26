"""Collection hooks for fs_scans tests."""

import subprocess

import pytest


# ---------------------------------------------------------------------------
# Shell-script test collection
# Any file matching *_test.sh in this directory tree is auto-collected and
# run as a pytest test item.  No per-script Python shim is needed — just
# drop a new *_test.sh file here and pytest picks it up automatically.
# ---------------------------------------------------------------------------

class ShellScriptError(Exception):
    def __init__(self, result):
        self.result = result


class ShellScriptItem(pytest.Item):
    def runtest(self):
        result = subprocess.run(
            ["bash", str(self.path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise ShellScriptError(result)

    def repr_failure(self, excinfo):
        r = excinfo.value.result
        lines = [f"Shell script failed (exit {r.returncode}): {self.path.name}"]
        if r.stdout.strip():
            lines += ["--- stdout ---", r.stdout.rstrip()]
        if r.stderr.strip():
            lines += ["--- stderr ---", r.stderr.rstrip()]
        return "\n".join(lines)

    def reportinfo(self):
        return self.path, None, f"shell: {self.path.name}"


class ShellScriptFile(pytest.File):
    def collect(self):
        yield ShellScriptItem.from_parent(self, name=self.path.name)


def pytest_collect_file(parent, file_path):
    """Collect *_test.sh files as shell-script test items."""
    if file_path.suffix == ".sh" and file_path.name.endswith("_test.sh"):
        return ShellScriptFile.from_parent(parent, path=file_path)
