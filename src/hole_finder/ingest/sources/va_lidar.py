"""Virginia LiDAR data source.

Downloads from VGIN (Virginia Geographic Information Network) via ArcGIS REST API.
Download URLs are wrapped in HTML anchor tags and need href extraction.
"""

import re
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

VA_TILE_INDEX_URL = (
    "https://vginmaps.vdem.virginia.gov/arcgis/rest/services/"
    "Download/Virginia_LiDAR_Downloads/MapServer/1/query"
)
_HREF_RE = re.compile(r'href="([^"]+)"')


def _extract_href(html: str) -> str | None:
    """Extract URL from an HTML anchor tag like <a href="...">text</a>."""
    m = _HREF_RE.search(html)
    return m.group(1) if m else None


class VALidarSource(DataSource):
    """Virginia LiDAR data from VGIN."""

    @property
    def name(self) -> str:
        return "va"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query VGIN tile index for tiles intersecting bbox."""
        bounds = bbox.bounds
        params = {
            "where": "1=1",
            "geometry": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "resultRecordCount": 500,
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.get(VA_TILE_INDEX_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("va_query_failed", error=str(e))
                return
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                geom = feature.get("geometry")
                tile_name = props.get("TileName") or props.get("Name") or str(props.get("FID", ""))
                raw_url = props.get("PointCloudDownload") or props.get("DEMDownload") or ""
                url = _extract_href(raw_url) if "<a " in raw_url else raw_url
                if url and geom:
                    from shapely.geometry import shape
                    yield TileInfo(
                        source_id=tile_name,
                        filename=f"va_{tile_name}.laz",
                        url=url,
                        bbox=shape(geom),
                        crs=26917,
                        format="laz",
                    )

    async def download_tile(self, tile: TileInfo, dest_dir: Path) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / tile.filename
        if dest_path.exists():
            return dest_path
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            async with client.stream("GET", tile.url) as response:
                response.raise_for_status()
                with open(dest_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 256):
                        f.write(chunk)
        return dest_path
