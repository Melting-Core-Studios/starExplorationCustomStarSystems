#!/usr/bin/env python3
"""Convert a CSV/TSV-like file into JSON array-of-objects.

Usage:
  python3 scripts/convert_csv_to_json.py input.raw output.json

Notes:
- NASA endpoints often return CSV unless format=json is specified.
- This converter auto-detects delimiter as comma or tab (fallback comma).
- Values are preserved as strings; type coercion is intentionally NOT performed.
"""

import csv
import json
import sys
from pathlib import Path

def sniff_delimiter(sample: str) -> str:
    if "\t" in sample and sample.count("\t") >= sample.count(","):
        return "\t"
    return ","

def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2

    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])

    text = inp.read_text("utf-8", errors="replace")
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")

    sample = text[:8192]
    delim = sniff_delimiter(sample)

    lines = text.splitlines()
    if not lines:
        raise SystemExit("Input is empty")

    reader = csv.DictReader(lines, delimiter=delim)
    rows = list(reader)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rows, ensure_ascii=False), "utf-8")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
