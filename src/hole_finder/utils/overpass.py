"""Shared Overpass API client with retry, mirror rotation, file caching, and rate limiting.

All Overpass queries in the codebase should go through query_overpass() to ensure
consistent reliability, caching, and observability.
"""

import hashlib
import json
import threading
import time
from pathlib import Path

import httpx
from httpx_retries import RetryTransport, Retry

from hole_finder.config import settings
from hole_finder.utils.log_manager import log

# --- Mirror rotation ---
# Each mirror has independent rate limits. On 429 from one, we try the next.
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
]

# --- File-based cache ---
# /data/hole-finder is a volume mount that survives container restarts.
CACHE_DIR = Path(getattr(settings, 'data_dir', '/data/hole-finder')) / "cache" / "overpass"
CACHE_TTL_S = 7 * 86400  # 7 days — OSM buildings/roads are very stable

# --- Rate limiter ---
# Overpass guideline: stay under ~1 req/s per IP. This lock ensures we don't
# burst 10-15 simultaneous requests when processing 5 tiles in parallel.
_rate_lock = threading.Lock()
_last_request_time = 0.0
_MIN_REQUEST_INTERVAL = 1.5  # seconds between requests


def _cache_key(query: str) -> str:
    """Deterministic cache key from query string."""
    return hashlib.sha256(query.strip().encode()).hexdigest()


def _get_cached(query: str) -> dict | None:
    """Return cached Overpass response if fresh, else None."""
    key = _cache_key(query)
    cache_file = CACHE_DIR / f"{key}.json"
    if not cache_file.exists():
        log.debug("overpass_cache_miss", key=key[:12])
        return None
    age_s = time.time() - cache_file.stat().st_mtime
    if age_s > CACHE_TTL_S:
        log.info("overpass_cache_expired", key=key[:12], age_days=round(age_s / 86400, 1), ttl_days=round(CACHE_TTL_S / 86400, 1))
        cache_file.unlink(missing_ok=True)
        return None
    try:
        data = json.loads(cache_file.read_text())
        log.info("overpass_cache_hit", key=key[:12], age_hours=round(age_s / 3600, 1))
        return data
    except Exception as e:
        log.warning("overpass_cache_read_corrupt", key=key[:12], error=str(e), exception=True)
        cache_file.unlink(missing_ok=True)
        return None


def _set_cached(query: str, data: dict) -> None:
    """Write Overpass response to file cache."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        key = _cache_key(query)
        cache_file = CACHE_DIR / f"{key}.json"
        raw = json.dumps(data)
        cache_file.write_text(raw)
        log.debug("overpass_cache_written", key=key[:12], size_kb=round(len(raw) / 1024, 1))
    except Exception as e:
        log.warning("overpass_cache_write_failed", error=str(e), exception=True)


def _rate_limit() -> None:
    """Block until at least _MIN_REQUEST_INTERVAL has passed since the last request."""
    global _last_request_time
    with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            wait_s = _MIN_REQUEST_INTERVAL - elapsed
            log.debug("overpass_rate_limit_wait", wait_s=round(wait_s, 2))
            time.sleep(wait_s)
        _last_request_time = time.monotonic()


def query_overpass(query: str, timeout: float = 60.0, query_label: str = "unknown") -> dict:
    """Execute an Overpass query with retry, mirror rotation, caching, and rate limiting.
    Args:
        query: The Overpass QL query string.
        timeout: HTTP request timeout in seconds.
        query_label: Human-readable label for logging (e.g. "buildings", "infrastructure").
    Returns:
        Parsed JSON response dict. Returns {"elements": []} on total failure.
    """
    # Check cache first
    cached = _get_cached(query)
    if cached is not None:
        return cached
    # Retry transport: 3 retries per mirror with exponential backoff
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], respect_retry_after_header=True)
    transport = RetryTransport(retry=retry)
    last_error = None
    for mirror_idx, mirror_url in enumerate(OVERPASS_MIRRORS):
        _rate_limit()
        t0 = time.perf_counter()
        log.info("overpass_request_start", mirror=mirror_url.split("//")[1].split("/")[0], query_label=query_label, mirror_idx=mirror_idx)
        try:
            with httpx.Client(transport=transport, timeout=timeout) as client:
                resp = client.post(mirror_url, data={"data": query})
                resp.raise_for_status()
            elapsed_ms = round((time.perf_counter() - t0) * 1000)
            data = resp.json()
            elements = data.get("elements", [])
            log.info("overpass_request_ok", mirror=mirror_url.split("//")[1].split("/")[0], query_label=query_label, elapsed_ms=elapsed_ms, elements=len(elements), response_kb=round(len(resp.content) / 1024, 1))
            _set_cached(query, data)
            return data
        except Exception as e:
            elapsed_ms = round((time.perf_counter() - t0) * 1000)
            last_error = str(e)
            log.warning("overpass_mirror_failed", mirror=mirror_url.split("//")[1].split("/")[0], query_label=query_label, elapsed_ms=elapsed_ms, error=last_error[:200], mirror_idx=mirror_idx)
            continue
    # All mirrors exhausted
    log.error("overpass_all_mirrors_exhausted", query_label=query_label, mirrors_tried=len(OVERPASS_MIRRORS), last_error=last_error[:200] if last_error else "unknown")
    return {"elements": []}
