"""Ohio OGRIP (Ohio Geographically Referenced Information Program) LiDAR source.

Downloads from OGRIP geodata download portal.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from magic_eyes.ingest.sources.base import DataSource, TileInfo
from magic_eyes.utils.logging import log

# OGRIP tile index endpoint
OGRIP_URL = (
    "https://gis1.oit.ohio.gov/arcgis/rest/services/OSIP/osip_best_available_dem/ImageServer/query"
)


class OHOGRIPSource(DataSource):
    """Ohio OGRIP LiDAR data."""

    @property
    def name(self) -> str:
        return "oh"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query OGRIP for tiles intersecting bbox."""
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
                response = await client.get(OGRIP_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("oh_query_failed", error=str(e))
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
                        filename=f"oh_{tile_name}.laz",
                        url=url,
                        bbox=shape(geom),
                        crs=3735,  # Ohio State Plane South
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
