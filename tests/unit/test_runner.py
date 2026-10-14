from __future__ import annotations

import sys
import unittest

from tminus.backend.runner import SubprocessRunner
from tminus.errors import CommandTimeoutError, SlurmCommandError


class SubprocessRunnerTest(unittest.TestCase):
    def test_captures_stdout_stderr_and_exit_status(self):
        script = "import sys; print('out'); print('err', file=sys.stderr); sys.exit(3)"

        result = SubprocessRunner().run([sys.executable, "-c", script])

        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stdout.strip(), "out")
        self.assertEqual(result.stderr.strip(), "err")

    def test_a_missing_executable_raises_a_command_error(self):
        with self.assertRaises(SlurmCommandError) as caught:
            SubprocessRunner().run(["tminus-no-such-command-xyz"])

        self.assertIsNone(caught.exception.returncode)
        self.assertIn("tminus-no-such-command-xyz", str(caught.exception))

    def test_a_command_that_outlives_its_timeout_raises(self):
        with self.assertRaises(CommandTimeoutError):
            SubprocessRunner().run([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.5)


if __name__ == "__main__":
    unittest.main()
