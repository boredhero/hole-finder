"""Kentucky LiDAR data source.

Downloads from KyFromAbove program via ArcGIS REST API.
Prefers Phase 2 COPC data (2022, newer) over Phase 1 (2012).
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

KY_TILE_INDEX_URL = (
    "https://kygisserver.ky.gov/arcgis/rest/services/"
    "WGS84WM_Services/KY_Data_Tiles_PointCloud_WGS84WM/MapServer/0/query"
)


class KYLidarSource(DataSource):
    """Kentucky LiDAR data from KyFromAbove."""

    @property
    def name(self) -> str:
        return "ky"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query KyFromAbove tile index for tiles intersecting bbox."""
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
                response = await client.get(KY_TILE_INDEX_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("ky_query_failed", error=str(e))
                return
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                geom = feature.get("geometry")
                tile_name = props.get("tilename") or props.get("Name") or str(props.get("FID", ""))
                # Prefer Phase 2 (newer, COPC), fall back to Phase 1
                url = props.get("phase2_aws_url") or props.get("phase1_aws_url") or ""
                fmt = "laz"
                if url and ".copc.laz" in url:
                    fmt = "copc.laz"
                if url and geom:
                    from shapely.geometry import shape
                    yield TileInfo(
                        source_id=tile_name,
                        filename=f"ky_{tile_name}.laz",
                        url=url,
                        bbox=shape(geom),
                        crs=3089,
                        format=fmt,
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
