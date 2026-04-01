"""Filter detections that fall on roads, waterways, or railways using OpenStreetMap data.

Uses the Overpass API (free, no auth) to fetch infrastructure polygons/lines
for a given bounding box, then excludes detections whose centroid falls inside
a buffered infrastructure feature. Springs are exempt from water filtering.
"""

import httpx
from shapely.geometry import LineString, MultiPolygon, Point, Polygon, shape
from shapely.ops import unary_union
from shapely.prepared import prep

from hole_finder.utils.logging import log

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
# Buffer distance in degrees (~15m at mid-latitudes) for line features (roads, rivers)
LINE_BUFFER_DEG = 0.00015


def fetch_infrastructure_polygons(
    west: float, south: float, east: float, north: float,
) -> dict[str, list[Polygon]]:
    """Fetch OSM roads, waterways, water bodies, and railways via Overpass API.

    Returns dict with keys 'roads', 'water', 'railways' → lists of buffered Shapely Polygons.
    """
    bbox = f"{south},{west},{north},{east}"
    query = f"""
    [out:json][timeout:45];
    (
      way["highway"~"motorway|trunk|primary|secondary|tertiary|motorway_link|trunk_link|primary_link|secondary_link"]({bbox});
      way["waterway"~"river|stream|canal|drain|ditch"]({bbox});
      way["natural"="water"]({bbox});
      relation["natural"="water"]({bbox});
      way["railway"~"rail|light_rail"]({bbox});
    );
    out geom;
    """
    try:
        resp = httpx.post(OVERPASS_URL, data={"data": query}, timeout=45.0)
        resp.raise_for_status()
    except Exception as e:
        log.warning("overpass_infrastructure_fetch_failed", error=str(e))
        return {"roads": [], "water": [], "railways": []}
    data = resp.json()
    elements = data.get("elements", [])
    roads: list[Polygon] = []
    water: list[Polygon] = []
    railways: list[Polygon] = []
    for el in elements:
        tags = el.get("tags", {})
        geom_nodes = el.get("geometry", [])
        members = el.get("members", [])
        # Handle ways (lines → buffer into polygons)
        if el.get("type") == "way" and len(geom_nodes) >= 2:
            try:
                coords = [(node["lon"], node["lat"]) for node in geom_nodes]
                if tags.get("natural") == "water" and len(coords) >= 4:
                    poly = Polygon(coords)
                    if poly.is_valid and poly.area > 0:
                        water.append(poly)
                        continue
                line = LineString(coords)
                buffered = line.buffer(LINE_BUFFER_DEG)
                if not buffered.is_valid or buffered.is_empty:
                    continue
                if tags.get("highway"):
                    roads.append(buffered)
                elif tags.get("waterway"):
                    water.append(buffered)
                elif tags.get("railway"):
                    railways.append(buffered)
            except Exception:
                continue
        # Handle relations (multipolygon water bodies like lakes)
        elif el.get("type") == "relation" and members:
            for member in members:
                if member.get("role") == "outer" and member.get("type") == "way":
                    member_geom = member.get("geometry", [])
                    if len(member_geom) >= 4:
                        try:
                            coords = [(n["lon"], n["lat"]) for n in member_geom]
                            poly = Polygon(coords)
                            if poly.is_valid and poly.area > 0:
                                water.append(poly)
                        except Exception:
                            continue
    log.info("infrastructure_fetched", roads=len(roads), water=len(water), railways=len(railways), bbox=f"{west},{south},{east},{north}")
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
    infra = fetch_infrastructure_polygons(west, south, east, north)
    all_road = infra["roads"]
    all_water = infra["water"]
    all_railway = infra["railways"]
    if not all_road and not all_water and not all_railway:
        return [(c, lon, lat) for c, (lon, lat) in zip(candidates, wgs84_coords)]
    road_geom = prep(unary_union(all_road)) if all_road else None
    water_geom = prep(unary_union(all_water)) if all_water else None
    rail_geom = prep(unary_union(all_railway)) if all_railway else None
    original = len(candidates)
    road_removed = 0
    water_removed = 0
    rail_removed = 0
    keep: list[tuple] = []
    for c, (lon, lat) in zip(candidates, wgs84_coords):
        pt = Point(lon, lat)
        is_spring = getattr(c, 'feature_type', None) and str(c.feature_type) == 'spring'
        if road_geom and road_geom.contains(pt):
            road_removed += 1
            continue
        if water_geom and not is_spring and water_geom.contains(pt):
            water_removed += 1
            continue
        if rail_geom and rail_geom.contains(pt):
            rail_removed += 1
            continue
        keep.append((c, lon, lat))
    log.info("infrastructure_filter_result", original=original, remaining=len(keep), road_removed=road_removed, water_removed=water_removed, rail_removed=rail_removed)
    return keep
