"""IngestManager — orchestrates tile discovery and download across data sources.

Source resolution: coordinates → FCC reverse geocode → state code → sources.
No region polygons needed for bbox/zip searches.
"""

from pathlib import Path

import httpx
from shapely.geometry import Polygon, shape

from hole_finder.config import settings
from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.ingest.sources.ct_lidar import CTLidarSource
from hole_finder.ingest.sources.ky_lidar import KYLidarSource
from hole_finder.ingest.sources.md_lidar import MDLidarSource
from hole_finder.ingest.sources.nc_lidar import NCLidarSource
from hole_finder.ingest.sources.nj_lidar import NJLidarSource
from hole_finder.ingest.sources.ny_lidar import NYLidarSource
from hole_finder.ingest.sources.oh_ogrip import OHOGRIPSource
from hole_finder.ingest.sources.pasda import PASDASource
from hole_finder.ingest.sources.tnm_lidar import TNMLidarSource
from hole_finder.ingest.sources.usgs_3dep import USGS3DEPSource
from hole_finder.ingest.sources.va_lidar import VALidarSource
from hole_finder.ingest.sources.wv_lidar import WVLidarSource
from hole_finder.utils.logging import log

# Registry of all available data sources
SOURCE_REGISTRY: dict[str, type[DataSource]] = {
    "usgs_3dep": USGS3DEPSource,
    "tnm": TNMLidarSource,
    "pasda": PASDASource,
    "wv": WVLidarSource,
    "ny": NYLidarSource,
    "oh": OHOGRIPSource,
    "nc": NCLidarSource,
    "md": MDLidarSource,
    "va": VALidarSource,
    "ky": KYLidarSource,
    "nj": NJLidarSource,
    "ct": CTLidarSource,
}

# State code → state-specific LiDAR sources (beyond usgs_3dep/tnm)
STATE_SOURCES: dict[str, list[str]] = {
    "PA": ["pasda"],
    "WV": ["wv"],
    "NY": ["ny"],
    "OH": ["oh"],
    "NC": ["nc"],
    "MD": ["md"],
    "VA": ["va"],
    "KY": ["ky"],
    "NJ": ["nj"],
    "CT": ["ct"],
}


def get_source(name: str) -> DataSource:
    """Get a data source instance by name."""
    if name not in SOURCE_REGISTRY:
        raise KeyError(f"Unknown source: {name!r}. Available: {list(SOURCE_REGISTRY.keys())}")
    return SOURCE_REGISTRY[name]()


def resolve_state(lat: float, lon: float) -> str | None:
    """Reverse geocode lat/lon to US state code via FCC Area API.
    Returns 2-letter state code (e.g. 'PA', 'NC') or None if outside US."""
    try:
        resp = httpx.get(
            "https://geo.fcc.gov/api/census/area",
            params={"lat": lat, "lon": lon, "format": "json"},
            timeout=5.0,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            return results[0].get("state_code")
    except Exception as e:
        log.warning("fcc_geocode_failed", lat=lat, lon=lon, error=str(e))
    return None


def get_sources_for_location(lat: float, lon: float) -> list[str]:
    """Determine data sources for a location: usgs_3dep first, then any
    state-specific source, then tnm as universal US fallback."""
    sources = ["usgs_3dep"]
    state = resolve_state(lat, lon)
    if state:
        log.info("state_resolved", state=state, lat=round(lat, 4), lon=round(lon, 4))
        for s in STATE_SOURCES.get(state, []):
            sources.append(s)
    sources.append("tnm")
    return sources


async def discover_tiles_for_bbox(bbox: Polygon, lat: float, lon: float) -> tuple[list[TileInfo], str]:
    """Discover tiles for a bbox, trying sources in order until one returns results.
    Skips TNM legacy tiles that lack CRS metadata (2001-era data with State Plane coords).
    Returns (tiles, source_name_used)."""
    sources = get_sources_for_location(lat, lon)
    for src_name in sources:
        src = get_source(src_name)
        tiles = []
        try:
            async for t in src.discover_tiles(bbox):
                tiles.append(t)
        except Exception as e:
            log.warning("source_discovery_failed", source=src_name, error=str(e))
        if tiles:
            # TNM can return legacy tiles (pre-2010) with no embedded CRS — these use
            # unknown State Plane coords and produce garbage results. Filter them out.
            # Modern TNM tiles (2010+) have proper CRS embedded in the LAZ headers.
            if src_name == "tnm":
                before = len(tiles)
                tiles = [t for t in tiles if t.format == "copc" or (t.acquisition_year and t.acquisition_year >= 2010)]
                if len(tiles) < before:
                    log.warning("tnm_legacy_filtered", before=before, after=len(tiles), dropped=before - len(tiles), reason="pre-2010 or unknown-year legacy tiles")
            if tiles:
                log.info("source_resolved", source=src_name, tiles=len(tiles))
                return tiles, src_name
            log.info("source_empty_after_filter", source=src_name)
    return [], "none"


async def download_tiles(
    tiles: list[TileInfo],
    source_name: str,
    dest_dir: Path | None = None,
) -> list[Path]:
    """Download a list of tiles from a specific source."""
    source = get_source(source_name)
    if dest_dir is None:
        dest_dir = settings.raw_dir / source_name
    paths = []
    for tile in tiles:
        try:
            path = await source.download_tile(tile, dest_dir)
            paths.append(path)
        except Exception as e:
            log.error("download_failed", tile=tile.source_id, error=str(e))
    return paths
