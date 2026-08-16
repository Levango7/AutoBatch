"""Project entry point.

Adds the project root to sys.path so the pipeline runs regardless of
launcher quirks (e.g. PYTHONSAFEPATH environments), then delegates to
src.pipeline.main.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))