"""Filter detections that fall on roads, waterways, or railways using OpenStreetMap data.

Uses the shared Overpass client (retry, mirror rotation, caching, rate limiting)
to fetch infrastructure polygons/lines for a given bounding box, then excludes
detections whose centroid falls inside a buffered feature. Springs are exempt
from water filtering.
"""

from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import unary_union
from shapely.prepared import prep

from hole_finder.utils.logging import log
from hole_finder.utils.overpass import query_overpass

# Buffer distances in degrees for line features
ROAD_BUFFER_DEG = 0.0003   # ~30m — highway cuts are wide
WATER_BUFFER_DEG = 0.0003  # ~30m — creek/river banks
RAIL_BUFFER_DEG = 0.0004   # ~40m — rail cuts through hills are wide


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
    data = query_overpass(query, timeout=60.0, query_label="infrastructure")
    elements = data.get("elements", [])
    if not elements:
        log.warning("overpass_no_infrastructure_returned", bbox=f"{west},{south},{east},{north}")
        return {"roads": [], "water": [], "railways": []}
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
                if tags.get("highway"):
                    roads.append(line.buffer(ROAD_BUFFER_DEG))
                elif tags.get("waterway"):
                    water.append(line.buffer(WATER_BUFFER_DEG))
                elif tags.get("railway"):
                    railways.append(line.buffer(RAIL_BUFFER_DEG))
                else:
                    continue
            except Exception as e:
                log.debug("infra_geom_parse_failed", element_type=el.get("type"), error=str(e))
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
