"""USGS TNMAccess LiDAR data source.

Universal fallback source using the USGS National Map Access API.
Covers all US states with individual LAZ tile download URLs.
Used for states without their own tile index services (VT, TN, IN, NH, ME, RI, DE).
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon, box

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

TNM_API_URL = "https://tnmaccess.nationalmap.gov/api/v1/products"


class TNMLidarSource(DataSource):
    """USGS TNMAccess LiDAR — universal US coverage."""

    @property
    def name(self) -> str:
        return "tnm"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query TNMAccess for LAZ tiles intersecting bbox."""
        bounds = bbox.bounds
        params = {
            "datasets": "Lidar Point Cloud (LPC)",
            "bbox": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            "prodFormats": "LAZ",
            "max": 50,
            "offset": 0,
            "outputFormat": "JSON",
        }
        seen = set()
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Paginate up to 500 tiles
            for page in range(10):
                params["offset"] = page * 50
                try:
                    response = await client.get(TNM_API_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                except (httpx.HTTPError, Exception) as e:
                    log.warning("tnm_query_failed", error=str(e), page=page)
                    return
                items = data.get("items", [])
                if not items:
                    return
                for item in items:
                    url = item.get("downloadLazURL") or item.get("downloadURL") or ""
                    title = item.get("title") or item.get("sourceId") or ""
                    bb = item.get("boundingBox", {})
                    if not url or not bb or url in seen:
                        continue
                    seen.add(url)
                    try:
                        tile_bbox = box(bb["minX"], bb["minY"], bb["maxX"], bb["maxY"])
                    except (KeyError, TypeError):
                        continue
                    # Extract a short tile name from the URL
                    tile_name = Path(url).stem
                    yield TileInfo(
                        source_id=tile_name,
                        filename=f"tnm_{tile_name}.laz",
                        url=url,
                        bbox=tile_bbox,
                        crs=4326,
                        file_size_bytes=item.get("sizeInBytes"),
                        format="laz",
                    )
                total = data.get("total", 0)
                if params["offset"] + 50 >= total:
                    return

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
