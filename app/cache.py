"""Query result caching with JSON persistence and TTL expiration.

CACHE LOCATION: app/data/query_cache.json
CACHE TTL: 180 seconds (configurable per set operation)

WORKFLOW:
1. PortIntelligenceService.query_port() creates cache key from query parameters
2. PersistentCache checks if key exists and is not expired
3. If expired, entry is deleted and None returned (trigger fresh query)
4. If valid, cached response restored and returned immediately
5. After fresh query completes, result stored with 180-second expiration

BENEFITS:
- Reduces API calls (if live endpoints configured)
- Immediateprovider faster responses for repeated queries
- Transparent to application layer
- JSON format allows manual inspection of cache

EXPIRATION:
- Each entry stores expiresAtUtc timestamp
- Checked on read; expired entries auto-deleted
- No background cleanup (lazy expiration on access)
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


@dataclass(slots=True)
class CacheEntry:
    """Single cache entry with expiration time."""
    expiresAtUtc: str  # ISO 8601 timestamp when this entry expires
    payload: dict  # Cached query response data


class PersistentCache:
    """JSON-based persistent cache with TTL support.
    
    Implements simple key-value store with automatic expiration.
    Uses SHA-256 hashing for cache keys.
    All operations are atomic (full file reads/writes).
    """

    def __init__(self, cache_file: str = "app/data/query_cache.json") -> None:
        """Initialize cache with file path.
        
        Creates parent directories and empty cache file if not exists.
        """
        self.cache_path = Path(cache_file)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.cache_path.exists():
            self.cache_path.write_text("{}", encoding="utf-8")

    def make_key(self, parts: list[str]) -> str:
        """Generate SHA-256 cache key from parts list.
        
        Joins parts with '|' separator, then hashes to ensure deterministic keys.
        """
        joined = "|".join(parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def get(self, key: str) -> dict | None:
        """Retrieve cached payload if not expired; clean up if expired.
        
        Returns:
        - dict: The cached response payload if found and not expired
        - None: If key not found or entry is expired (and deleted)
        """
        data = self._read_all()
        row = data.get(key)
        if not row:
            return None

        # Check expiration
        expiry = _parse_iso(row.get("expiresAtUtc", ""))
        if expiry is None or expiry < datetime.now(UTC):
            # Auto-delete expired entry
            data.pop(key, None)
            self._write_all(data)
            return None

        return row.get("payload")

    def set(self, key: str, payload: dict, ttl_seconds: int) -> None:
        """Store cached payload with TTL.
        
        Computes expiration timestamp and stores entry.
        If key exists, overwrites with new payload and expiration.
        """
        data = self._read_all()
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        data[key] = asdict(CacheEntry(expiresAtUtc=expires_at.isoformat(), payload=payload))
        self._write_all(data)

    def _read_all(self) -> dict:
        """Read entire cache from file (atomic read)."""
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _write_all(self, data: dict) -> None:
        """Write entire cache to file (atomic write with compact JSON)."""
        self.cache_path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def _parse_iso(value: str) -> datetime | None:
    """Parse ISO 8601 datetime string (handles both 'Z' and '+00:00' formats)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


