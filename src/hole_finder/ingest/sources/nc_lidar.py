"""North Carolina LiDAR data source.

Uses NOAA Coastal LiDAR S3 bucket (NC 2015 Phase 3 statewide COPC).
Dataset 6209: 0.7m point spacing, LAS 1.4, NAD83(2011)/UTM zone 17N.
Tiles organized by county folders on S3.
"""

import re
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon, box

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

NOAA_S3_BASE = "https://noaa-nos-coastal-lidar-pds.s3.amazonaws.com"
NOAA_S3_PREFIX = "laz/geoid18/6209"
# County folder names on S3 follow pattern: "{County}CoNC"
# FCC API returns "Guilford County" → we convert to "GuilfordCoNC"


def _county_to_s3_folder(county_name: str) -> str:
    """Convert FCC county name to NOAA S3 folder name.
    'Guilford County' → 'GuilfordCoNC', 'Wake County' → 'WakeCoNC'"""
    name = county_name.replace(" County", "").replace(" ", "")
    return f"{name}CoNC"


async def _resolve_county(lat: float, lon: float) -> str | None:
    """Resolve lat/lon to county name via FCC Census Area API."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("https://geo.fcc.gov/api/census/area", params={"lat": lat, "lon": lon, "format": "json"})
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                return results[0].get("county_name")
    except Exception as e:
        log.warning("fcc_county_resolve_failed", lat=lat, lon=lon, error=str(e))
    return None


async def _list_s3_tiles(county_folder: str) -> list[tuple[str, str]]:
    """List all COPC tile keys+URLs from a county folder on S3.
    Returns list of (tile_name, full_url)."""
    tiles = []
    prefix = f"{NOAA_S3_PREFIX}/{county_folder}/"
    marker = ""
    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(20):  # Max 20 pages of 1000
            params = {"prefix": prefix, "max-keys": "1000"}
            if marker:
                params["marker"] = marker
            resp = await client.get(f"{NOAA_S3_BASE}/", params=params)
            resp.raise_for_status()
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            contents = root.findall('.//s3:Contents', ns)
            if not contents:
                break
            for c in contents:
                key = c.find('s3:Key', ns).text
                if key.endswith('.copc.laz'):
                    tile_name = Path(key).stem.replace(".copc", "")
                    url = f"{NOAA_S3_BASE}/{key}"
                    tiles.append((tile_name, url))
                marker = key
            is_truncated = root.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated', 'false')
            if is_truncated.lower() != 'true':
                break
    return tiles


class NCLidarSource(DataSource):
    """North Carolina LiDAR from NOAA S3 (2015 Phase 3 statewide COPC)."""

    @property
    def name(self) -> str:
        return "nc"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Discover NC COPC tiles by resolving bbox center to county, listing S3."""
        centroid = bbox.centroid
        county_name = await _resolve_county(centroid.y, centroid.x)
        if not county_name:
            log.warning("nc_no_county", lat=centroid.y, lon=centroid.x)
            return
        folder = _county_to_s3_folder(county_name)
        log.info("nc_s3_discovery", county=county_name, folder=folder)
        try:
            tile_list = await _list_s3_tiles(folder)
        except Exception as e:
            log.warning("nc_s3_list_failed", folder=folder, error=str(e))
            return
        if not tile_list:
            log.info("nc_s3_no_tiles", folder=folder)
            return
        log.info("nc_s3_tiles_found", folder=folder, count=len(tile_list))
        # Yield all tiles with the search bbox as approximate coverage.
        # The task's distance sorting will pick the nearest ones.
        for tile_name, url in tile_list:
            yield TileInfo(
                source_id=tile_name,
                filename=f"nc_{tile_name}.copc.laz",
                url=url,
                bbox=bbox,  # Approximate — all tiles share the search bbox
                crs=6543,   # NAD83(2011) / UTM zone 17N
                acquisition_year=2015,
                format="copc",
            )

    async def download_tile(self, tile: TileInfo, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / tile.filename
        if dest_path.exists():
            return dest_path
        log.info("downloading_tile", dest=str(dest_path), url=tile.url[:120])
        async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
            async with client.stream("GET", tile.url) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))
                downloaded = 0
                with open(dest_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 256):
                        f.write(chunk)
                        downloaded += len(chunk)
                log.info("download_complete", path=str(dest_path), bytes=downloaded, expected=total)
        return dest_path
