import re
from typing import Optional

_LOOM_ID_RE = re.compile(r"\b([a-f0-9]{32})\b", re.IGNORECASE)
_URL_ID_RE = re.compile(
    r"(?:https?://)?(?:www\.)?loom\.com/(?:share|embed|recording)/([a-f0-9]{32})",
    re.IGNORECASE,
)

def extract_video_id(item: str) -> Optional[str]:
    """
    Extract a Loom 32-char hex video ID from a URL or a raw ID string.

    Returns normalized lowercase ID or None if not found.
    """
    if not item:
        return None

    # Try URL pattern first
    m = _URL_ID_RE.search(item.strip())
    if m:
        return m.group(1).lower()

    # Fallback: raw 32-hex token anywhere in string
    m = _LOOM_ID_RE.search(item.strip())
    if m:
        return m.group(1).lower()

    return None

def is_probably_timestamp(token: str) -> bool:
    """Heuristically determine if a string looks like a timestamp like 00:12 or 01:02:03"""
    if not token:
        return False
    return bool(re.fullmatch(r"(?:\d{1,2}:)?\d{1,2}:\d{2}(?:,\d{1,3})?", token))

def normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace while keeping single newlines."""
    # Replace CRLF with LF, collapse multiple spaces, trim lines
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Remove trailing spaces per line
    text = "\n".join(line.strip() for line in text.split("\n"))
    # Collapse consecutive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()