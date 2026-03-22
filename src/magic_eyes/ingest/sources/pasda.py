"""Pennsylvania Spatial Data Access (PASDA) LiDAR source.

Downloads from PASDA via their ArcGIS REST API tile index + direct HTTP.
PAMAP program tiles are on a 10,000ft x 10,000ft grid.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon, box, shape

from magic_eyes.ingest.sources.base import DataSource, TileInfo
from magic_eyes.utils.logging import log

# PASDA ArcGIS REST endpoint for PAMAP LiDAR tile index
PASDA_TILE_INDEX_URL = (
    "https://maps.psiee.psu.edu/arcgis/rest/services/PAMAP/PAMAP_LiDAR/MapServer/0/query"
)


class PASDASource(DataSource):
    """Pennsylvania PAMAP LiDAR data from PASDA."""

    @property
    def name(self) -> str:
        return "pasda"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query PASDA tile index for tiles intersecting bbox."""
        bounds = bbox.bounds  # (minx, miny, maxx, maxy)

        params = {
            "where": "1=1",
            "geometry": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "resultRecordCount": 1000,
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            log.info("pasda_query", bbox=bounds)
            response = await client.get(PASDA_TILE_INDEX_URL, params=params)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            log.info("pasda_results", count=len(features))

            for feature in features:
                props = feature.get("properties", {})
                geom = feature.get("geometry")
                if not geom:
                    continue

                tile_name = props.get("TILE_NAME") or props.get("Name") or props.get("FID", "unknown")
                download_url = props.get("DOWNLOAD_URL") or props.get("URL", "")

                if not download_url:
                    # Construct URL from tile name pattern
                    download_url = f"https://www.pasda.psu.edu/uci/LidarDownload.aspx?tile={tile_name}"

                yield TileInfo(
                    source_id=str(tile_name),
                    filename=f"{tile_name}.laz",
                    url=download_url,
                    bbox=shape(geom) if geom.get("type") else box(*bounds),
                    crs=2272,  # PA State Plane South (NAD83)
                    file_size_bytes=props.get("FILE_SIZE"),
                    acquisition_year=props.get("YEAR"),
                    format="laz",
                )

    async def download_tile(self, tile: TileInfo, dest_dir: Path) -> Path:
        """Download a PASDA LAZ tile via HTTP."""
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / tile.filename

        if dest_path.exists():
            return dest_path

        log.info("downloading_pasda_tile", tile=tile.source_id, url=tile.url)

        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            async with client.stream("GET", tile.url) as response:
                response.raise_for_status()
                with open(dest_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 256):
                        f.write(chunk)

        return dest_path
