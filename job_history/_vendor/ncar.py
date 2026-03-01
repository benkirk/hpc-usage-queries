"""Shim: makes job_history._vendor.ncar importable for pickle compatibility.

The actual source lives in pbs-parser-ncar/ncar.py; the hyphenated directory
name prevents direct dotted-path import.  This shim loads it via importlib and
replaces itself in sys.modules so that only one DerechoRecord class ever exists.

Both sync (via _get_record_class) and query (via JobRecord.to_pbs_record /
pickle.loads) resolve through sys.modules["job_history._vendor.ncar"], ensuring
they always see the same class object and deserialization succeeds.
"""
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
import sys

_path = Path(__file__).parent / "pbs-parser-ncar" / "ncar.py"
_spec = spec_from_file_location(__name__, _path)
_mod = module_from_spec(_spec)
sys.modules[__name__] = _mod   # replace shim with real module before exec
_spec.loader.exec_module(_mod)
