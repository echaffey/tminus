"""Test suite for tminus. Run with ``python -m unittest`` from the repo root.

Importing this package puts ``src/`` on ``sys.path`` so the tests run against
the checkout with nothing installed (the target HPC restricts pip), and points
``TMINUS_DB`` at a throwaway database so no test touches a real one.
"""
import atexit
import os
import shutil
import sys
import tempfile

_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB_DIR = tempfile.mkdtemp(prefix="tminus-tests-")
atexit.register(shutil.rmtree, _DB_DIR, ignore_errors=True)
os.environ["TMINUS_DB"] = os.path.join(_DB_DIR, "jobs.db")
