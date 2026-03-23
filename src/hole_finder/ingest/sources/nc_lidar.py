"""North Carolina LiDAR data source.

Downloads from NC OneMap tile index via ArcGIS REST API.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

# NC OneMap statewide tile index endpoint
NC_TILE_INDEX_URL = (
    "https://services.nconemap.gov/secure/rest/services/"
    "ImageryProject/Statewide_Tile_Index/MapServer/0/query"
)


class NCLidarSource(DataSource):
    """North Carolina LiDAR data from NC OneMap."""

    @property
    def name(self) -> str:
        return "nc"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query NC tile index for tiles intersecting bbox."""
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
                response = await client.get(NC_TILE_INDEX_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("nc_query_failed", error=str(e))
                return

            for feature in data.get("features", []):
                props = feature.get("properties", {})
                geom = feature.get("geometry")

                tile_name = props.get("Name") or props.get("TILE") or str(props.get("FID", ""))
                url = props.get("URL") or props.get("DOWNLOAD_URL", "")

                if url and geom:
                    from shapely.geometry import shape
                    yield TileInfo(
                        source_id=tile_name,
                        filename=f"nc_{tile_name}.laz",
                        url=url,
                        bbox=shape(geom),
                        crs=32119,  # NAD83 / North Carolina
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
