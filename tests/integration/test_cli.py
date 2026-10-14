from __future__ import annotations

import contextlib
import io
import os
import unittest
from unittest import mock

from tminus.cli import main


def run_cli(*argv, env=None):
    """Run the CLI in-process; return (exit status, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.dict(os.environ, env or {"TMINUS_BACKEND": "fake"}), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            status = main(list(argv))
        except SystemExit as exc:
            status = exc.code
    return status, out.getvalue(), err.getvalue()


class SubmitCommandTest(unittest.TestCase):
    def test_submitting_a_wrapped_command_prints_the_job_id(self):
        status, out, _ = run_cli("submit", "--cpus", "4", "--mem", "8G", "--wrap", "echo hi")

        self.assertEqual(status, 0)
        self.assertRegex(out.strip(), r"^\d+$")

    def test_submitting_a_script_prints_the_job_id(self):
        status, out, _ = run_cli("submit", "--partition", "debug", "job.sh")

        self.assertEqual(status, 0)
        self.assertRegex(out.strip(), r"^\d+$")

    def test_submit_without_a_command_or_script_is_a_usage_error(self):
        status, _, err = run_cli("submit", "--cpus", "4")

        self.assertEqual(status, 2)
        self.assertIn("usage", err)

    def test_an_unknown_backend_is_reported_without_a_traceback(self):
        status, _, err = run_cli("submit", "--wrap", "true", env={"TMINUS_BACKEND": "nope"})

        self.assertEqual(status, 1)
        self.assertIn("nope", err)
        self.assertNotIn("Traceback", err)


if __name__ == "__main__":
    unittest.main()
