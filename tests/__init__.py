"""Test suite for tminus. Run with ``python -m unittest`` from the repo root.

Importing this package puts ``src/`` on ``sys.path`` so the tests run against
the checkout with nothing installed (the target HPC restricts pip).
"""
import os
import sys

_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
