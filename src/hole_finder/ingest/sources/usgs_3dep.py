"""USGS 3DEP LiDAR source via STAC API + COPC from Planetary Computer.

Data is free and requires no API key. Uses pystac-client to query the
Microsoft Planetary Computer STAC catalog for 3DEP COPC tiles.

Download URLs require SAS token signing (Azure blob storage has public
access disabled). The planetary-computer SDK handles this automatically
via a free, unauthenticated token endpoint.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import planetary_computer
from shapely.geometry import Polygon, shape

from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.utils.logging import log

# Planetary Computer STAC endpoint (free, no auth for search)
STAC_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION_ID = "3dep-lidar-copc"


class USGS3DEPSource(DataSource):
    """USGS 3DEP LiDAR data via Planetary Computer STAC + COPC."""

    @property
    def name(self) -> str:
        return "usgs_3dep"

    async def discover_tiles(self, bbox: Polygon) -> AsyncIterator[TileInfo]:
        """Query STAC catalog for COPC tiles intersecting the bounding box."""
        bounds = bbox.bounds  # (minx, miny, maxx, maxy)

        search_body = {
            "collections": [COLLECTION_ID],
            "bbox": list(bounds),
            "limit": 250,
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            url = f"{STAC_API_URL}/search"
            page = 1

            while True:
                log.info("stac_search", url=url, page=page, bbox=bounds)
                response = await client.post(url, json=search_body)
                response.raise_for_status()
                data = response.json()

                features = data.get("features", [])
                if not features:
                    break

                for item in features:
                    tile_info = self._parse_stac_item(item)
                    if tile_info:
                        yield tile_info

                # Follow next link for pagination
                next_link = None
                for link in data.get("links", []):
                    if link.get("rel") == "next":
                        next_link = link.get("body") or link.get("href")
                        break

                if next_link and isinstance(next_link, dict):
                    search_body = next_link
                    page += 1
                elif next_link and isinstance(next_link, str):
                    url = next_link
                    page += 1
                else:
                    break

    def _parse_stac_item(self, item: dict) -> TileInfo | None:
        """Parse a STAC item into TileInfo."""
        try:
            item_id = item["id"]
            geometry = shape(item["geometry"])
            properties = item.get("properties", {})

            # Find the COPC asset
            assets = item.get("assets", {})
            copc_asset = assets.get("data") or assets.get("copc")
            if not copc_asset:
                return None

            href = copc_asset["href"]

            # Try to get file size from asset
            file_size = copc_asset.get("file:size")

            # Extract acquisition year from datetime
            dt_str = properties.get("datetime", "")
            year = None
            if dt_str and len(dt_str) >= 4:
                try:
                    year = int(dt_str[:4])
                except ValueError:
                    pass

            return TileInfo(
                source_id=item_id,
                filename=f"{item_id}.copc.laz",
                url=href,
                bbox=geometry,
                crs=properties.get("proj:epsg", 4326),
                file_size_bytes=file_size,
                acquisition_year=year,
                format="copc",
            )
        except (KeyError, TypeError) as e:
            log.warning("stac_parse_error", item_id=item.get("id"), error=str(e))
            return None

    async def download_tile(self, tile: TileInfo, dest_dir: Path) -> Path:
        """Download a COPC tile via HTTP with Planetary Computer SAS signing.

        Azure blob storage requires a SAS token. planetary_computer.sign()
        appends the token to the URL. Tokens are free, no account needed,
        cached for ~24h by the SDK.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / tile.filename

        if dest_path.exists():
            log.info("tile_exists", path=str(dest_path))
            return dest_path

        # Sign the URL with a Planetary Computer SAS token
        signed_url = planetary_computer.sign(tile.url)
        log.info("downloading_tile", url=signed_url[:120], dest=str(dest_path))

        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            async with client.stream("GET", signed_url) as response:
                response.raise_for_status()
                content_length = int(response.headers.get("content-length", 0))

                with open(dest_path, "wb") as f:
                    downloaded = 0
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 256):
                        f.write(chunk)
                        downloaded += len(chunk)

        log.info("download_complete", path=str(dest_path),
                 bytes=downloaded, expected=content_length)
        return dest_path
