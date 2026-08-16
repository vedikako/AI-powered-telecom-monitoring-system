from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ml.evaluate import evaluate


def main() -> None:
    result = evaluate()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
