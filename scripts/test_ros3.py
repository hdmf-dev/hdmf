#!/usr/bin/env python
"""Run the ros3 tests and recover from the HDF5 ros3 shutdown deadlock on Windows.

The ros3 tests pass and then the interpreter hangs at shutdown, so the job burns the full
GitHub Actions runtime limit. This script runs pytest in a child process. Once the child has
reported its exit code but has not exited, the parent terminates it and propagates the code.
The parent holds no HDF5 state, so it is free of the loader lock the child is stuck on.

Arguments are passed through to pytest.

See https://github.com/hdmf-dev/hdmf/issues/1571 and
https://github.com/HDFGroup/hdf5/issues/6560
"""

import os
import subprocess
import sys
import tempfile
import time

CHILD_ENV_VAR = "HDMF_ROS3_EXITCODE_FILE"
RESULT_GRACE = 30       # seconds to allow a clean exit after the child reports its result
STARTUP_TIMEOUT = 1800  # fail if the child never reports a result


def read_exitcode(path: str) -> int | None:
    """Return the exit code the child recorded, or None if it has not recorded one yet."""
    try:
        with open(path) as f:
            content = f.read().strip()
    except OSError:
        return None
    if not content:
        return None
    try:
        return int(content)
    except ValueError:
        return None


def run_tests(args: list[str]) -> int:
    """Run pytest, record the exit code where the parent can see it, and return it."""
    import pytest

    code = int(pytest.main(args))
    with open(os.environ[CHILD_ENV_VAR], "w") as f:
        f.write(str(code))
    return code


def supervise(args: list[str]) -> int:
    """Run the tests in a child process and return its exit code, killing it if it hangs."""
    fd, result_path = tempfile.mkstemp(prefix="hdmf_ros3_exitcode_")
    os.close(fd)
    env = dict(os.environ, **{CHILD_ENV_VAR: result_path})
    proc = subprocess.Popen([sys.executable, os.path.abspath(__file__)] + args, env=env)

    start = time.monotonic()
    reported = None
    reported_at = None
    try:
        while True:
            rc = proc.poll()
            if rc is not None:
                return rc

            if reported_at is None:
                reported = read_exitcode(result_path)
                if reported is not None:
                    reported_at = time.monotonic()

            if reported_at is not None and time.monotonic() - reported_at > RESULT_GRACE:
                print("pytest reported exit code %d but did not exit within %ds; terminating it"
                      % (reported, RESULT_GRACE), flush=True)
                proc.kill()
                proc.wait()
                return reported

            if reported_at is None and time.monotonic() - start > STARTUP_TIMEOUT:
                print("pytest did not report an exit code within %ds; terminating it"
                      % STARTUP_TIMEOUT, flush=True)
                proc.kill()
                proc.wait()
                return 1

            time.sleep(1)
    finally:
        try:
            os.remove(result_path)
        except OSError:
            pass


if __name__ == "__main__":
    if CHILD_ENV_VAR in os.environ:
        sys.exit(run_tests(sys.argv[1:]))
    sys.exit(supervise(sys.argv[1:]))
