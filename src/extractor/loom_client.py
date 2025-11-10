import json
import os
import re
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class LoomError(RuntimeError):
    pass

class LoomClient:
    """
    Lightweight Loom client that tries multiple strategies to obtain a transcript:

    1) Known/observed caption endpoints (if available).
    2) Parse the share page HTML for embedded JSON with transcript/captions.
    3) Parse player config JSON within inline <script> tags.

    This client requires public accessibility of the target Loom recording.
    """

    def __init__(self, user_agent: str, timeout_seconds: int = 20, proxy: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent or "LoomTranscriptScraper/1.0",
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            }
        )
        self.timeout = timeout_seconds
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        self._bearer = os.getenv("LOOM_TOKEN")

        if self._bearer:
            self.session.headers["Authorization"] = f"Bearer {self._bearer}"

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.8, min=1, max=6),
        retry=retry_if_exception_type((requests.RequestException, LoomError)),
    )
    def fetch_transcript_text(self, video_id: str) -> str:
        """
        Try multiple strategies and return raw transcript text (may contain timestamps/labels).
        Raises LoomError if nothing workable is found.
        """
        # Strategy 1: Caption/Transcript API candidates
        apis = [
            f"https://www.loom.com/api/v1/captions/{video_id}",  # legacy guess
            f"https://www.loom.com/api/captions/transcript/{video_id}",  # newer guess
            f"https://www.loom.com/api/v1/videos/{video_id}/transcript",
        ]
        for url in apis:
            text = self._try_fetch_json_transcript(url)
            if text:
                return text

        # Strategy 2: Scrape share page for embedded transcript
        share_url = f"https://www.loom.com/share/{video_id}"
        html = self._get(share_url)
        text = self._parse_share_page_for_transcript(html)
        if text:
            return text

        # Strategy 3: Player page variations
        player = f"https://www.loom.com/embed/{video_id}"
        html = self._get(player)
        text = self._parse_share_page_for_transcript(html)
        if text:
            return text

        raise LoomError("Transcript not found or video is private/unavailable.")

    def _try_fetch_json_transcript(self, url: str) -> Optional[str]:
        try:
            resp = self.session.get(url, timeout=self.timeout, proxies=self.proxies)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError):
            return None

        # Heuristics across potential shapes
        if isinstance(data, dict):
            # Common shapes: {"transcript": "..."} or {"captions":[{"text": "..."}]}
            if "transcript" in data and isinstance(data["transcript"], str):
                return data["transcript"]

            if "captions" in data and isinstance(data["captions"], list):
                fragments = []
                for c in data["captions"]:
                    txt = c.get("text") or c.get("caption") or ""
                    if txt:
                        fragments.append(txt)
                return "\n".join(fragments).strip() if fragments else None

            # Some APIs nest under 'data'
            if "data" in data:
                inner = data["data"]
                if isinstance(inner, dict) and "transcript" in inner and isinstance(inner["transcript"], str):
                    return inner["transcript"]

        if isinstance(data, list):
            # Perhaps list of caption fragments
            fragments = []
            for c in data:
                if isinstance(c, dict):
                    txt = c.get("text") or c.get("caption") or ""
                else:
                    txt = str(c)
                if txt:
                    fragments.append(txt)
            return "\n".join(fragments).strip() if fragments else None

        return None

    def _parse_share_page_for_transcript(self, html: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")

        # Look for <script> containing "transcript"
        candidates = soup.find_all("script")
        for script in candidates:
            content = script.string or script.text or ""
            if not content or "transcript" not in content.lower():
                continue

            # Try to extract JSON blocks in the script
            for blob in self._extract_json_like_strings(content):
                txt = self._extract_text_from_json_blob(blob)
                if txt:
                    return txt

        # Search for elements possibly holding text tracks
        # (fallback heuristics)
        text_nodes = soup.find_all(text=re.compile(r"\btranscript\b", re.IGNORECASE))
        for node in text_nodes:
            s = str(node)
            if len(s) > 40:
                return s

        return None

    def _extract_json_like_strings(self, s: str):
        # Greedy braces extraction; tries to parse big JSON-ish structures
        stack = []
        start = None
        for i, ch in enumerate(s):
            if ch == "{":
                if not stack:
                    start = i
                stack.append("{")
            elif ch == "}":
                if stack:
                    stack.pop()
                    if not stack and start is not None:
                        yield s[start : i + 1]

    def _extract_text_from_json_blob(self, blob: str) -> Optional[str]:
        try:
            data = json.loads(blob)
        except Exception:
            # Some blobs may be JS, try to coerce quotes
            coerced = re.sub(r"(?<!\\)'", '"', blob)
            try:
                data = json.loads(coerced)
            except Exception:
                return None

        # Heuristic walk to find transcript-like content
        queue = [data]
        fragments = []
        seen = 0
        while queue and seen < 20000:
            seen += 1
            node = queue.pop(0)
            if isinstance(node, dict):
                # Obvious keys first
                for key in ("transcript", "captions", "subtitles", "srt", "vtt", "text"):
                    if key in node:
                        val = node[key]
                        if isinstance(val, str) and len(val) > 20:
                            fragments.append(val)
                        elif isinstance(val, list):
                            for item in val:
                                if isinstance(item, (str, int, float)) and str(item).strip():
                                    fragments.append(str(item))
                                else:
                                    queue.append(item)
                        elif isinstance(val, dict):
                            queue.append(val)
                # Enqueue children
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        queue.append(v)
            elif isinstance(node, list):
                for v in node:
                    if isinstance(v, (dict, list)):
                        queue.append(v)
                    elif isinstance(v, (str, int, float)) and str(v).strip():
                        fragments.append(str(v))

        joined = "\n".join(fragments).strip()
        return joined if len(joined) > 40 else None

    def _get(self, url: str) -> str:
        resp = self.session.get(url, timeout=self.timeout, proxies=self.proxies)
        # 4xx from private videos should bubble up to retries/handler
        if resp.status_code in (401, 403, 404):
            raise LoomError(f"Unavailable: {resp.status_code}")
        resp.raise_for_status()
        return resp.text