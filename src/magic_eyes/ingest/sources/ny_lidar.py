"""New York State LiDAR data source.

Downloads from NYS GIS Clearinghouse / NOAA Digital Coast.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from magic_eyes.ingest.sources.base import DataSource, TileInfo
from magic_eyes.utils.logging import log

# NY GIS LiDAR tile index endpoint
NY_LIDAR_URL = (
    "https://services6.arcgis.com/DZHaqZm9elBK5cUF/ArcGIS/rest/services/"
    "NYS_Lidar/FeatureServer/0/query"
)


class NYLidarSource(DataSource):
    """New York State LiDAR data."""

    @property
    def name(self) -> str:
        return "ny"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query NY tile index for tiles intersecting bbox."""
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
                response = await client.get(NY_LIDAR_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("ny_query_failed", error=str(e))
                return

            for feature in data.get("features", []):
                props = feature.get("properties", {})
                geom = feature.get("geometry")

                tile_name = props.get("Tile") or props.get("NAME") or str(props.get("FID", ""))
                url = props.get("LAS_URL") or props.get("URL", "")

                if url and geom:
                    from shapely.geometry import shape
                    yield TileInfo(
                        source_id=tile_name,
                        filename=f"ny_{tile_name}.laz",
                        url=url,
                        bbox=shape(geom),
                        crs=26918,  # UTM 18N
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
