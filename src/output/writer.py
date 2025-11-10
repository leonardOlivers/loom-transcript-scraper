import json
import sys
from typing import List, Dict, Optional

def write_json(rows: List[Dict], path: Optional[str] = None, pretty: bool = True) -> None:
    """
    Write list of dicts as JSON to a file or stdout.
    """
    if pretty:
        payload = json.dumps(rows, ensure_ascii=False, indent=2)
    else:
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))

    if path:
        with open(path, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
    else:
        sys.stdout.write(payload + "\n")
        sys.stdout.flush()