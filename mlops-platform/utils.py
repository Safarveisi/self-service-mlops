"""
Some helper functions
"""

import logging
import subprocess
import sys
from threading import Timer


def get_logger(name: str = "myapp") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:  # Avoid duplicate handlers on reload
        return logger
    logger.setLevel(logging.INFO)

    h = logging.StreamHandler(sys.stderr)
    h.setLevel(logging.INFO)
    h.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.addHandler(h)
    logger.propagate = False  # Don't bubble to root (prevents other libs)
    return logger


def run_command(cmd: list[str], timeout_seconds: int, env: dict | None = None) -> None:
    """
    Runs the specified command. If it exits with non-zero status, `RuntimeError` is raised.
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    timer = Timer(timeout_seconds, proc.kill)
    try:
        timer.start()
        stdout, stderr = proc.communicate()
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        if proc.returncode != 0:
            msg = "\n".join(
                [
                    f"Encountered an unexpected error while running {cmd}",
                    f"exit status: {proc.returncode}",
                    f"stdout: {stdout}",
                    f"stderr: {stderr}",
                ]
            )
            raise RuntimeError(msg)
    finally:
        if timer.is_alive():
            timer.cancel()
