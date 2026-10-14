"""Guards the Python 3.7 floor when the suite runs on a newer interpreter.

Parses every source file with 3.7 grammar and rejects APIs that 3.7 lacks. On a
real 3.7 interpreter the import of each module is the actual check, so this
test is skipped there (``ast.parse`` only gained ``feature_version`` in 3.8).
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Names that do not exist in 3.7's typing module.
FORBIDDEN_TYPING_NAMES = {"Literal", "Protocol", "TypedDict", "Final", "final", "Annotated", "TypeAlias"}
# Attribute and function names introduced after 3.7.
FORBIDDEN_ATTRIBUTES = {"removeprefix", "removesuffix", "to_thread", "cached_property", "is_relative_to"}
# Keyword arguments introduced after 3.7, by the function that gained them.
FORBIDDEN_KEYWORDS = {"dataclass": {"slots", "kw_only"}, "zip": {"strict"}}


def _called_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def python_sources():
    """Every Python file that ships or tests: src/, tests/, and bin/ launchers."""
    for top in ("src", "tests"):
        for dirpath, _, filenames in os.walk(os.path.join(ROOT, top)):
            for filename in filenames:
                if filename.endswith(".py"):
                    yield os.path.join(dirpath, filename)
    bin_dir = os.path.join(ROOT, "bin")
    if os.path.isdir(bin_dir):
        for filename in os.listdir(bin_dir):
            yield os.path.join(bin_dir, filename)


def problems_in(source):
    """Return a list of 3.7-incompatibility descriptions for ``source``."""
    try:
        tree = ast.parse(source, feature_version=(3, 7))
    except SyntaxError as exc:
        return [f"line {exc.lineno}: not valid 3.7 syntax: {exc.msg}"]
    problems = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "typing":
            for alias in node.names:
                if alias.name in FORBIDDEN_TYPING_NAMES:
                    problems.append(f"line {node.lineno}: typing.{alias.name} is 3.8+")
        elif isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ATTRIBUTES:
            problems.append(f"line {node.lineno}: .{node.attr} is 3.8+")
        elif isinstance(node, (ast.Name, ast.alias)):
            name = node.id if isinstance(node, ast.Name) else node.name
            if name in FORBIDDEN_ATTRIBUTES:
                problems.append(f"line {node.lineno}: {name} is 3.8+")
        elif isinstance(node, ast.Call):
            forbidden = FORBIDDEN_KEYWORDS.get(_called_name(node), set())
            for keyword in node.keywords:
                if keyword.arg in forbidden:
                    problems.append(f"line {node.lineno}: {_called_name(node)}({keyword.arg}=) is 3.10+")
    return problems


@unittest.skipIf(sys.version_info < (3, 8), "running on 3.7 itself, which is the real check")
class Python37CompatibilityTest(unittest.TestCase):
    def test_the_checker_rejects_known_3_8_plus_code(self):
        known_bad = [
            "if (n := 1): pass",
            "def f(a, /): pass",
            "match x:\n    case 1: pass",
            "from typing import Literal",
            "s.removeprefix('a')",
            "from functools import cached_property",
            "@dataclass(slots=True)\nclass C: pass",
            "zip(a, b, strict=True)",
        ]
        for snippet in known_bad:
            with self.subTest(snippet=snippet):
                self.assertTrue(problems_in(snippet))

    def test_the_checker_accepts_same_named_keywords_that_3_7_supports(self):
        self.assertEqual(problems_in("json.loads(s, strict=False)"), [])

    def test_every_source_file_is_python_3_7_compatible(self):
        for path in python_sources():
            with self.subTest(path=os.path.relpath(path, ROOT)):
                with open(path, encoding="utf-8") as handle:
                    self.assertEqual(problems_in(handle.read()), [])

    def test_every_package_module_postpones_annotation_evaluation(self):
        for path in python_sources():
            if os.sep + "src" + os.sep not in path:
                continue
            with self.subTest(path=os.path.relpath(path, ROOT)):
                with open(path, encoding="utf-8") as handle:
                    self.assertIn("from __future__ import annotations", handle.read())


if __name__ == "__main__":
    unittest.main()
