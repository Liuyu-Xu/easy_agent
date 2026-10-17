"""Run the deterministic suite and the user-authorized real API regression."""

import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--round", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    env_file = str(Path(args.env_file).resolve())
    evidence = str(Path(args.evidence).resolve())
    if Path(evidence).exists():
        print('Evidence path already exists; choose a new round file.')
        return 1
    root = Path(__file__).resolve().parents[1]
    deterministic = subprocess.run(
        [sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests", "-q"],
        cwd=root,
    )
    if deterministic.returncode:
        return deterministic.returncode
    live = subprocess.run(
        [sys.executable, "-B", str(root / "tests" / "live_api.py"),
         "--env-file", env_file, "--round", args.round, "--evidence", evidence],
        cwd=root,
    )
    return live.returncode


if __name__ == "__main__":
    raise SystemExit(main())
