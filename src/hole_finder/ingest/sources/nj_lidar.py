"""New Jersey LiDAR data source.

Downloads from NJGIN via ArcGIS REST FeatureServers.
NJ splits data into per-region services; we query the most relevant ones.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from shapely.geometry import Polygon

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

# NJ has separate FeatureServers per LiDAR project
NJ_SERVICES = [
    ("https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Grid_LiDAR_Northwest_2018/FeatureServer/0/query", "nw2018"),
    ("https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Grid_LiDAR_South_2019/FeatureServer/0/query", "south2019"),
    ("https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Grid_LiDAR_DVRPC_2015/FeatureServer/0/query", "dvrpc2015"),
    ("https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Grid_LiDAR_NortheastNJPostSandy_2014/FeatureServer/0/query", "ne2014"),
]


class NJLidarSource(DataSource):
    """New Jersey LiDAR data from NJGIN."""

    @property
    def name(self) -> str:
        return "nj"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query NJ tile indices for tiles intersecting bbox."""
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
        seen = set()
        async with httpx.AsyncClient(timeout=60.0) as client:
            for url, tag in NJ_SERVICES:
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()
                except (httpx.HTTPError, Exception) as e:
                    log.warning("nj_query_failed", service=tag, error=str(e))
                    continue
                for feature in data.get("features", []):
                    props = feature.get("properties", {})
                    geom = feature.get("geometry")
                    tile_name = props.get("Name") or props.get("TILE") or str(props.get("FID", ""))
                    dl_url = props.get("Download_Link") or props.get("URL") or ""
                    key = f"{tag}_{tile_name}"
                    if dl_url and geom and key not in seen:
                        seen.add(key)
                        from shapely.geometry import shape
                        yield TileInfo(
                            source_id=key,
                            filename=f"nj_{key}.laz",
                            url=dl_url,
                            bbox=shape(geom),
                            crs=3424,
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
