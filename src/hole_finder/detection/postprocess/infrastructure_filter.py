"""Filter detections that fall on roads, waterways, or railways using offline OSM data.

Uses locally-stored Geofabrik PBF + osmium CLI to get infrastructure features,
then excludes detections whose centroid falls inside a buffered feature.
Springs are exempt from water filtering.
"""

import time

from shapely.geometry import Point
from shapely.ops import unary_union
from shapely.prepared import prep

from hole_finder.utils.log_manager import log
from hole_finder.utils.osm_data import get_railway_geometries, get_road_geometries, get_water_geometries

# Buffer distances in degrees for line features
ROAD_BUFFER_DEG = 0.0003   # ~30m — highway cuts are wide
WATER_BUFFER_DEG = 0.0003  # ~30m — creek/river banks
RAIL_BUFFER_DEG = 0.0004   # ~40m — rail cuts through hills are wide


def _buffer_lines(geometries: list, buffer_deg: float) -> list:
    """Buffer line geometries into polygons. Polygons pass through unchanged."""
    result = []
    for geom in geometries:
        if geom.geom_type in ("LineString", "MultiLineString"):
            result.append(geom.buffer(buffer_deg))
        elif geom.geom_type in ("Polygon", "MultiPolygon"):
            result.append(geom)
        else:
            result.append(geom.buffer(buffer_deg))
    return result


def fetch_infrastructure_polygons(west: float, south: float, east: float, north: float) -> dict[str, list]:
    """Fetch OSM roads, waterways, water bodies, and railways from offline PBF data.
    Returns dict with keys 'roads', 'water', 'railways' -> lists of buffered Shapely Polygons.
    """
    log.debug("infrastructure_fetch_start", bbox=f"{west},{south},{east},{north}")
    t0 = time.monotonic()
    raw_roads = get_road_geometries(west, south, east, north)
    raw_water = get_water_geometries(west, south, east, north)
    raw_railways = get_railway_geometries(west, south, east, north)
    elapsed = time.monotonic() - t0
    roads = _buffer_lines(raw_roads, ROAD_BUFFER_DEG) if raw_roads else []
    water = _buffer_lines(raw_water, WATER_BUFFER_DEG) if raw_water else []
    railways = _buffer_lines(raw_railways, RAIL_BUFFER_DEG) if raw_railways else []
    log.info("infrastructure_fetched", roads=len(roads), water=len(water), railways=len(railways), bbox=f"{west},{south},{east},{north}", elapsed_s=round(elapsed, 3))
    return {"roads": roads, "water": water, "railways": railways}


def filter_candidates_by_infrastructure(
    candidates: list,
    wgs84_coords: list[tuple[float, float]],
    west: float, south: float, east: float, north: float,
) -> list[tuple]:
    """Remove candidates on roads, water, or railways. Springs exempt from water filter.
    Args:
        candidates: list of Candidate objects
        wgs84_coords: list of (lon, lat) tuples matching candidates
        west/south/east/north: WGS84 bounding box
    Returns:
        list of (candidate, lon, lat) tuples that passed all filters
    """
    log.debug("infrastructure_filter_start", candidate_count=len(candidates), bbox=f"{west},{south},{east},{north}")
    infra = fetch_infrastructure_polygons(west, south, east, north)
    all_road = infra["roads"]
    all_water = infra["water"]
    all_railway = infra["railways"]
    if not all_road and not all_water and not all_railway:
        log.info("infrastructure_filter_skipped", reason="no_infrastructure_found", candidate_count=len(candidates))
        return [(c, lon, lat) for c, (lon, lat) in zip(candidates, wgs84_coords)]
    road_geom = prep(unary_union(all_road)) if all_road else None
    water_geom = prep(unary_union(all_water)) if all_water else None
    rail_geom = prep(unary_union(all_railway)) if all_railway else None
    original = len(candidates)
    road_removed = 0
    water_removed = 0
    rail_removed = 0
    spring_exemptions = 0
    keep: list[tuple] = []
    for c, (lon, lat) in zip(candidates, wgs84_coords):
        pt = Point(lon, lat)
        is_spring = getattr(c, 'feature_type', None) and str(c.feature_type) == 'spring'
        if road_geom and road_geom.contains(pt):
            road_removed += 1
            continue
        if water_geom and water_geom.contains(pt):
            if is_spring:
                spring_exemptions += 1
            else:
                water_removed += 1
                continue
        if rail_geom and rail_geom.contains(pt):
            rail_removed += 1
            continue
        keep.append((c, lon, lat))
    log.info("infrastructure_filter_result", original=original, remaining=len(keep), road_removed=road_removed, water_removed=water_removed, rail_removed=rail_removed, spring_exemptions=spring_exemptions)
    return keep
