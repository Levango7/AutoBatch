"""Regenerate demo data only (no pipeline run).

Usage: python tools/gen_data.py --config config/pipeline.json
"""

import argparse
import sys

sys.path.insert(0, ".")

from src.generator import main as gen_main  # noqa: E402
from src.helpers import abs_path, json_load  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate demo data")
    parser.add_argument("--config", default="config/pipeline.json")
    args = parser.parse_args(sys.argv[1:])
    result = gen_main(json_load(abs_path(args.config)))
    print("generated:", result["rows"])
