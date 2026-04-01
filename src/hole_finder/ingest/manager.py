"""IngestManager — orchestrates tile discovery and download across data sources."""

from pathlib import Path

from shapely.geometry import Polygon, shape

from hole_finder.config import settings
from hole_finder.ingest.sources.base import DataSource, TileInfo
from hole_finder.ingest.sources.md_lidar import MDLidarSource
from hole_finder.ingest.sources.nc_lidar import NCLidarSource
from hole_finder.ingest.sources.ny_lidar import NYLidarSource
from hole_finder.ingest.sources.oh_ogrip import OHOGRIPSource
from hole_finder.ingest.sources.pasda import PASDASource
from hole_finder.ingest.sources.usgs_3dep import USGS3DEPSource
from hole_finder.ingest.sources.wv_lidar import WVLidarSource
from hole_finder.utils.logging import log

# Registry of all available data sources
SOURCE_REGISTRY: dict[str, type[DataSource]] = {
    "usgs_3dep": USGS3DEPSource,
    "pasda": PASDASource,
    "wv": WVLidarSource,
    "ny": NYLidarSource,
    "oh": OHOGRIPSource,
    "nc": NCLidarSource,
    "md": MDLidarSource,
}


def get_source(name: str) -> DataSource:
    """Get a data source instance by name."""
    if name not in SOURCE_REGISTRY:
        raise KeyError(f"Unknown source: {name!r}. Available: {list(SOURCE_REGISTRY.keys())}")
    return SOURCE_REGISTRY[name]()


def get_sources_for_region(region_name: str) -> list[str]:
    """Determine which data sources to use for a named region."""
    region_sources = {
        "western_pa": ["usgs_3dep", "pasda"],
        "eastern_pa": ["usgs_3dep", "pasda"],
        "west_virginia": ["usgs_3dep", "wv"],
        "eastern_ohio": ["usgs_3dep", "oh"],
        "upstate_ny": ["usgs_3dep", "ny"],
        "western_nc": ["usgs_3dep", "nc"],
        "western_md": ["usgs_3dep", "md"],
        "western_ma": ["usgs_3dep"],
        "south_louisiana": ["usgs_3dep"],
        "north_louisiana": ["usgs_3dep"],
        "northern_ca_lava": ["usgs_3dep"],
        "sierra_nevada": ["usgs_3dep"],
        "southern_ca_desert": ["usgs_3dep"],
    }
    return region_sources.get(region_name, ["usgs_3dep"])


def get_sources_for_bbox(bbox: Polygon) -> list[str]:
    """Determine which data sources cover a bbox by checking region overlaps.
    Always includes usgs_3dep first, then appends any state-specific sources
    whose region polygon intersects the given bbox."""
    sources = ["usgs_3dep"]
    seen = {"usgs_3dep"}
    region_source_map = {
        "western_pa": ["pasda"], "eastern_pa": ["pasda"],
        "west_virginia": ["wv"], "eastern_ohio": ["oh"],
        "upstate_ny": ["ny"], "western_nc": ["nc"], "western_md": ["md"],
    }
    for region_name, extra_sources in region_source_map.items():
        try:
            region_poly = load_region_bbox(region_name)
            if region_poly.intersects(bbox):
                for s in extra_sources:
                    if s not in seen:
                        sources.append(s)
                        seen.add(s)
        except FileNotFoundError:
            continue
    return sources


def load_region_bbox(region_name: str) -> Polygon:
    """Load a region's bounding polygon from the configs/regions/ GeoJSON."""
    import json
    region_file = Path(__file__).parent.parent.parent.parent / "configs" / "regions" / f"{region_name}.geojson"
    if not region_file.exists():
        raise FileNotFoundError(f"Region file not found: {region_file}")
    with open(region_file) as f:
        data = json.load(f)
    # Support both Feature and FeatureCollection
    if data.get("type") == "FeatureCollection":
        geom = data["features"][0]["geometry"]
    elif data.get("type") == "Feature":
        geom = data["geometry"]
    else:
        geom = data
    return shape(geom)


async def discover_region(region_name: str) -> list[TileInfo]:
    """Discover all available tiles for a region across relevant sources."""
    bbox = load_region_bbox(region_name)
    source_names = get_sources_for_region(region_name)
    all_tiles = []

    for source_name in source_names:
        source = get_source(source_name)
        log.info("discovering_tiles", source=source_name, region=region_name)
        try:
            async for tile in source.discover_tiles(bbox):
                all_tiles.append(tile)
        except Exception as e:
            log.error("discovery_failed", source=source_name, error=str(e))

    log.info("discovery_complete", region=region_name, total_tiles=len(all_tiles))
    return all_tiles


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
