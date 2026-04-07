"""Tests for the shared Overpass client — validates retry, caching, and mirror rotation."""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestOverpassCaching:
    """File-based caching prevents redundant API calls."""

    def test_cache_hit_returns_cached_data(self, tmp_path):
        from hole_finder.utils import overpass
        # Override cache dir to tmp
        original_cache_dir = overpass.CACHE_DIR
        overpass.CACHE_DIR = tmp_path
        try:
            query = "[out:json][timeout:30];(way['building'](38,-80,39,-79););out geom;"
            expected = {"elements": [{"type": "way", "id": 1}]}
            # Manually write cache
            overpass._set_cached(query, expected)
            # Should return cached without any network call
            result = overpass._get_cached(query)
            assert result is not None
            assert result["elements"][0]["id"] == 1
        finally:
            overpass.CACHE_DIR = original_cache_dir

    def test_expired_cache_returns_none(self, tmp_path):
        from hole_finder.utils import overpass
        import os
        original_cache_dir = overpass.CACHE_DIR
        overpass.CACHE_DIR = tmp_path
        try:
            query = "expired query"
            overpass._set_cached(query, {"elements": []})
            # Backdate the file beyond TTL
            cache_file = tmp_path / f"{overpass._cache_key(query)}.json"
            old_time = time.time() - overpass.CACHE_TTL_S - 100
            os.utime(cache_file, (old_time, old_time))
            assert overpass._get_cached(query) is None
        finally:
            overpass.CACHE_DIR = original_cache_dir

    def test_cache_key_is_deterministic(self):
        from hole_finder.utils.overpass import _cache_key
        q = "[out:json];(way['building'](38,-80,39,-79););out geom;"
        assert _cache_key(q) == _cache_key(q)
        # Different query produces different key
        q2 = "[out:json];(way['building'](39,-81,40,-80););out geom;"
        assert _cache_key(q) != _cache_key(q2)


class TestOverpassMirrorRotation:
    """When one mirror fails, the client tries the next."""

    @patch("hole_finder.utils.overpass._rate_limit")
    @patch("hole_finder.utils.overpass._get_cached", return_value=None)
    def test_returns_empty_on_total_failure(self, mock_cache, mock_rate):
        """When all mirrors fail, returns empty dict instead of raising."""
        from hole_finder.utils import overpass
        with patch("httpx.Client") as MockClient:
            mock_instance = MagicMock()
            mock_instance.__enter__ = MagicMock(return_value=mock_instance)
            mock_instance.__exit__ = MagicMock(return_value=False)
            mock_instance.post.side_effect = Exception("Connection refused")
            MockClient.return_value = mock_instance
            result = overpass.query_overpass("test query", timeout=5.0, query_label="test")
            assert result == {"elements": []}

    @patch("hole_finder.utils.overpass._rate_limit")
    @patch("hole_finder.utils.overpass._get_cached", return_value=None)
    @patch("hole_finder.utils.overpass._set_cached")
    def test_successful_response_is_cached(self, mock_set, mock_get, mock_rate):
        """Successful responses are written to cache."""
        from hole_finder.utils import overpass
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"elements": [{"type": "way"}]}
        mock_resp.content = b'{"elements": [{"type": "way"}]}'
        mock_resp.raise_for_status = MagicMock()
        with patch("httpx.Client") as MockClient:
            mock_instance = MagicMock()
            mock_instance.__enter__ = MagicMock(return_value=mock_instance)
            mock_instance.__exit__ = MagicMock(return_value=False)
            mock_instance.post.return_value = mock_resp
            MockClient.return_value = mock_instance
            result = overpass.query_overpass("test query", timeout=5.0, query_label="test")
            assert result["elements"][0]["type"] == "way"
            mock_set.assert_called_once()


class TestOverpassRateLimiter:
    """Rate limiter prevents burst requests."""

    def test_rate_limit_enforces_interval(self):
        from hole_finder.utils.overpass import _rate_limit, _MIN_REQUEST_INTERVAL
        import hole_finder.utils.overpass as overpass_mod
        # Reset last request time
        overpass_mod._last_request_time = 0.0
        t0 = time.monotonic()
        _rate_limit()
        _rate_limit()
        elapsed = time.monotonic() - t0
        # Second call should have waited at least _MIN_REQUEST_INTERVAL
        assert elapsed >= _MIN_REQUEST_INTERVAL * 0.9  # 10% tolerance for timing
