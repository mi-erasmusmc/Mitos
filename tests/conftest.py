import sys
from pathlib import Path

# Ensure project root is importable when running tests from within the tests/ package.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
