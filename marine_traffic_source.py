"""
Small, single-file data-source adapter for MarineTraffic ports data.

This keeps integration minimal and focused:
- Uses the provided MarineTraffic URL as the source.
- Tries to parse embedded JSON first.
- Falls back to parsing an HTML table when available.

Note: MarineTraffic may block automated access (consent wall / anti-bot).
"""

from __future__ import annotations

import json
import os
import re
import csv
from dataclasses import dataclass
from html import unescape
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MARINE_TRAFFIC_PORTS_URL = (
    "https://www.marinetraffic.com/en/data/?asset_type=ports&columns="
    "flag,portname,unlocode,photo,vessels_in_port,vessels_departures,"
    "vessels_arrivals,vessels_expected_arrivals,local_time,anchorage,"
    "geographical_area_one,geographical_area_two,coverage"
)


@dataclass
class MarineTrafficDataSource:
    # Keep defaults close to the class so this can be dropped into any project fast.
    url: str = MARINE_TRAFFIC_PORTS_URL
    timeout_seconds: int = 20
    cookie_header: str | None = None
    local_html_path: str = "mt_page.html"

    def fetch_ports(self, limit: int | None = 50) -> list[dict[str, Any]]:
        """Return normalized port records from the configured URL."""
        # Optional offline/manual source 1: exported CSV from MarineTraffic.
        csv_path = os.getenv("MARINETRAFFIC_PORTS_CSV", "").strip()
        if not csv_path:
            csv_path = self._auto_discover_csv_path()

        if csv_path:
            csv_records = self._read_ports_csv(csv_path)
            normalized = [self._normalize_record(r) for r in csv_records]
            return normalized[:limit] if limit is not None else normalized

        # Optional offline/manual source 2: saved page HTML.
        html = self._load_local_html_file()
        live_fetch_error = ""
        if not html:
            # Step 1: pull the page HTML from MarineTraffic.
            try:
                html = self._download_html()
            except RuntimeError as exc:
                live_fetch_error = str(exc)

        # Step 2: prefer JSON because it is usually cleaner and easier to map.
        records = self._parse_embedded_json(html)
        # Step 3: if JSON is missing, try a simple HTML table parse as backup.
        if not records:
            records = self._parse_html_table(html)

        if not records:
            source_hint = (
                f"Live fetch failed: {live_fetch_error}. "
                if live_fetch_error
                else ""
            )
            raise RuntimeError(
                f"Could not parse port data. {source_hint}"
                "Use one of these approaches: "
                "(1) save the MarineTraffic page source into mt_page.html, "
                "or (2) export CSV and set MARINETRAFFIC_PORTS_CSV."
            )

        # Final step: normalize keys so the rest of the project gets a stable schema.
        normalized = [self._normalize_record(r) for r in records]
        return normalized[:limit] if limit is not None else normalized

    def _read_ports_csv(self, csv_path: str) -> list[dict[str, Any]]:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]

    def _auto_discover_csv_path(self) -> str:
        # Heuristic search for a recent ports export to reduce manual setup.
        candidates: list[str] = []
        downloads_dir = os.path.expanduser("~\\Downloads")
        for base in [downloads_dir, os.getcwd()]:
            if not os.path.isdir(base):
                continue
            for name in os.listdir(base):
                lower = name.lower()
                if not lower.endswith(".csv"):
                    continue
                if any(token in lower for token in ["port", "marine", "traffic"]):
                    candidates.append(os.path.join(base, name))

        if not candidates:
            return ""

        candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return candidates[0]

    def _download_html(self) -> str:
        # Browser-like headers reduce the chance of being rejected immediately.
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        # If a valid browser cookie is provided, include it for consent/session access.
        cookie_value = self.cookie_header or os.getenv("MARINETRAFFIC_COOKIE", "").strip()
        if cookie_value:
            headers["Cookie"] = cookie_value

        req = Request(self.url, headers=headers)

        try:
            with urlopen(req, timeout=self.timeout_seconds) as response:
                return response.read().decode("utf-8", errors="ignore")
        except HTTPError as exc:
            if exc.code == 403:
                raise RuntimeError(
                    "HTTP 403 (blocked). MarineTraffic is rejecting automated requests."
                ) from exc
            raise RuntimeError(f"HTTP error while fetching MarineTraffic data: {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"Network error while fetching MarineTraffic data: {exc.reason}") from exc

    def _load_local_html_file(self) -> str:
        path = os.getenv("MARINETRAFFIC_HTML_FILE", self.local_html_path).strip()
        if not path or not os.path.isfile(path):
            return ""

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read().strip()
        return content

    def _parse_embedded_json(self, html: str) -> list[dict[str, Any]]:
        # Look for common in-page JSON payload patterns.
        json_blocks = re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.DOTALL | re.IGNORECASE)

        for block in json_blocks:
            # Quick filter: skip scripts that clearly are not related to port rows.
            if "portname" not in block.lower() and "unlocode" not in block.lower():
                continue

            # Try direct JSON object/array extraction from script content.
            candidates = re.findall(r"(\{.*\}|\[.*\])", block, flags=re.DOTALL)
            for candidate in candidates:
                parsed = self._safe_json_load(candidate)
                if isinstance(parsed, list):
                    # Keep only dictionary-like rows.
                    dict_rows = [r for r in parsed if isinstance(r, dict)]
                    if dict_rows:
                        return dict_rows
                if isinstance(parsed, dict):
                    # Sometimes data is nested; search recursively.
                    rows = self._find_record_list(parsed)
                    if rows:
                        return rows

        return []

    def _parse_html_table(self, html: str) -> list[dict[str, Any]]:
        # Minimal table parser to avoid external dependencies in this one-file drop-in.
        table_match = re.search(r"<table[^>]*>(.*?)</table>", html, flags=re.DOTALL | re.IGNORECASE)
        if not table_match:
            return []

        table_html = table_match.group(1)
        row_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, flags=re.DOTALL | re.IGNORECASE)
        if len(row_blocks) < 2:
            return []

        headers = [self._clean_html(cell) for cell in re.findall(r"<th[^>]*>(.*?)</th>", row_blocks[0], flags=re.DOTALL | re.IGNORECASE)]
        if not headers:
            return []

        # Build row dictionaries using the header labels as keys.
        rows: list[dict[str, Any]] = []
        for row in row_blocks[1:]:
            cells = [self._clean_html(cell) for cell in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, flags=re.DOTALL | re.IGNORECASE)]
            if not cells:
                continue
            rows.append(dict(zip(headers, cells)))

        return rows

    @staticmethod
    def _safe_json_load(text: str) -> Any:
        # Intentionally quiet: many script blocks are not valid JSON.
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    def _find_record_list(self, obj: Any) -> list[dict[str, Any]]:
        # Recursive search helps when the payload shape changes between releases.
        if isinstance(obj, list):
            if obj and all(isinstance(item, dict) for item in obj):
                return obj
            for item in obj:
                found = self._find_record_list(item)
                if found:
                    return found
            return []

        if isinstance(obj, dict):
            key_hits = {"portname", "unlocode", "vessels_in_port"}
            if key_hits.intersection({k.lower() for k in obj.keys()}):
                return [obj]

            for value in obj.values():
                found = self._find_record_list(value)
                if found:
                    return found

        return []

    @staticmethod
    def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
        # Keeps keys aligned with the requested MarineTraffic column names.
        # Also keeps `raw` so downstream logic can inspect original values if needed.
        return {
            "flag": record.get("flag"),
            "portname": record.get("portname") or record.get("Port Name") or record.get("port"),
            "unlocode": record.get("unlocode") or record.get("UNLOCODE"),
            "photo": record.get("photo"),
            "vessels_in_port": record.get("vessels_in_port"),
            "vessels_departures": record.get("vessels_departures"),
            "vessels_arrivals": record.get("vessels_arrivals"),
            "vessels_expected_arrivals": record.get("vessels_expected_arrivals"),
            "local_time": record.get("local_time"),
            "anchorage": record.get("anchorage"),
            "geographical_area_one": record.get("geographical_area_one"),
            "geographical_area_two": record.get("geographical_area_two"),
            "coverage": record.get("coverage"),
            "raw": record,
        }

    @staticmethod
    def _clean_html(text: str) -> str:
        # Strip tags and compress whitespace so table text is readable.
        text = re.sub(r"<[^>]+>", " ", text)
        text = unescape(text)
        return re.sub(r"\s+", " ", text).strip()


if __name__ == "__main__":
    # Tiny local demo: run this file directly to verify the adapter works.
    source = MarineTrafficDataSource()
    try:
        ports = source.fetch_ports(limit=5)
        print(json.dumps(ports, indent=2))
    except Exception as exc:
        print(f"Failed to fetch MarineTraffic ports data: {exc}")