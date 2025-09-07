#!/usr/bin/env python3
"""
Pylint runner script for the eventy project.

This script provides a convenient way to run pylint with the project's configuration.
"""

import subprocess
import sys
from pathlib import Path


def main():
    """Run pylint on the eventy package."""
    project_root = Path(__file__).parent.parent
    eventy_package = project_root / "eventy"

    if not eventy_package.exists():
        print(f"Error: Package directory {eventy_package} not found")
        sys.exit(1)

    # Run pylint with poetry
    cmd = ["poetry", "run", "pylint", str(eventy_package), "--output-format=text"]

    print(f"Running: {' '.join(cmd)}")
    print("-" * 60)

    try:
        result = subprocess.run(cmd, cwd=project_root, check=False)
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        print("\nLinting interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error running pylint: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
