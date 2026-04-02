"""Connecticut LiDAR data source.

Downloads from CT ECO via ArcGIS REST API.
Prefers 2023 LAZ data over 2016 LAS when available.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

CT_TILE_INDEX_URL = (
    "https://cteco.uconn.edu/ctraster/rest/services/"
    "maps/download_grids/MapServer/1/query"
)


class CTLidarSource(DataSource):
    """Connecticut LiDAR data from CT ECO."""

    @property
    def name(self) -> str:
        return "ct"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query CT ECO tile index for tiles intersecting bbox."""
        bounds = bbox.bounds
        # CT ECO does not support resultRecordCount
        params = {
            "where": "1=1",
            "geometry": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.get(CT_TILE_INDEX_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("ct_query_failed", error=str(e))
                return
            for feature in data.get("features", []):
                props = feature.get("properties", {})
                geom = feature.get("geometry")
                tile_name = props.get("Tile") or props.get("name") or str(props.get("FID", ""))
                # Prefer 2023 LAZ over 2016 LAS
                url = props.get("elev_2023_laz") or props.get("elev_2016_las") or ""
                if url and geom:
                    from shapely.geometry import shape
                    yield TileInfo(
                        source_id=tile_name,
                        filename=f"ct_{tile_name}.laz",
                        url=url,
                        bbox=shape(geom),
                        crs=6434,
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
