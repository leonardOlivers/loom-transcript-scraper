import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

from extractor.loom_client import LoomClient, LoomError
from extractor.transcript_cleaner import clean_transcript
from extractor.utils import extract_video_id
from output.writer import write_json

def load_settings(config_dir: Path) -> Dict[str, Any]:
    # Prefer settings.json if user created it; fall back to example
    user_cfg = config_dir / "settings.json"
    example_cfg = config_dir / "settings.example.json"
    for p in (user_cfg, example_cfg):
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
    # Hard defaults
    return {
        "user_agent": "LoomTranscriptScraper/1.0",
        "timeout_seconds": 20,
        "max_retries": 3,
        "concurrent_workers": 4,
        "respect_robots_txt": False,
        "proxy": None,
        "output_pretty": True,
    }

def parse_input(input_path: Path) -> List[str]:
    """
    Accept a JSON file with an array of strings (URLs or IDs).
    """
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return [str(x) for x in data]
    elif isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
        return [str(x) for x in data["items"]]
    else:
        raise ValueError("Input JSON must be an array of URLs/IDs or an object with 'items' array.")

def process_one(client: LoomClient, item: str) -> Tuple[str, str]:
    """
    Returns (videoId, cleanedTranscript) on success; raises on failure.
    """
    vid = extract_video_id(item)
    if not vid:
        raise ValueError(f"Could not extract Loom video ID from '{item}'")

    raw = client.fetch_transcript_text(vid)
    cleaned = clean_transcript(raw)
    if not cleaned:
        raise LoomError("Transcript extracted but empty after cleaning.")

    return vid, cleaned

def main():
    parser = argparse.ArgumentParser(
        description="Extract clean transcripts from Loom videos by URL or ID."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=True,
        help="Path to JSON file containing an array of Loom URLs or video IDs.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Path to write output JSON. If omitted, prints to stdout.",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=None,
        help="Number of concurrent workers (overrides config).",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    config = load_settings(base_dir / "config")

    ua = config.get("user_agent", "LoomTranscriptScraper/1.0")
    timeout = int(config.get("timeout_seconds", 20))
    proxy = config.get("proxy")
    pretty = bool(config.get("output_pretty", True))
    workers = args.workers or int(config.get("concurrent_workers", 4))

    client = LoomClient(user_agent=ua, timeout_seconds=timeout, proxy=proxy)

    items = parse_input(Path(args.input))
    if not items:
        print("No inputs provided.", file=sys.stderr)
        sys.exit(1)

    results: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_map = {pool.submit(process_one, client, it): it for it in items}
        for fut in as_completed(future_map):
            original = future_map[fut]
            try:
                vid, transcript = fut.result()
                results.append({"videoId": vid, "transcript": transcript})
                print(f"[OK] {vid}", file=sys.stderr)
            except Exception as e:
                errors.append({"input": original, "error": str(e)})
                print(f"[ERR] {original} -> {e}", file=sys.stderr)

    # Write outputs
    write_json(results, args.output, pretty=pretty)

    # If any failures, also emit a sidecar error file next to --output (or stderr)
    if errors:
        if args.output:
            err_path = Path(args.output).with_suffix(".errors.json")
            write_json(errors, str(err_path), pretty=True)
            print(f"Wrote error report to {err_path}", file=sys.stderr)
        else:
            print("\nErrors:", file=sys.stderr)
            for e in errors:
                print(json.dumps(e, ensure_ascii=False), file=sys.stderr)

    # Exit code reflects partial success: 0 if some results, 2 if none
    sys.exit(0 if results else 2)

if __name__ == "__main__":
    main()