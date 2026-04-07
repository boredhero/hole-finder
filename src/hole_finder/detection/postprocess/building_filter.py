"""Filter detections that fall on building footprints using OpenStreetMap data.

Uses the shared Overpass client (retry, mirror rotation, caching, rate limiting)
to fetch building polygons, then excludes any detection whose centroid falls
inside a building. Buildings inside cemeteries are kept (mausoleums/crypts don't
produce false LiDAR anomalies, and real features can exist beneath them).
"""

from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.prepared import prep

from hole_finder.utils.logging import log
from hole_finder.utils.overpass import query_overpass


def _parse_polygons_from_elements(elements: list[dict]) -> list[Polygon]:
    """Extract valid Shapely Polygons from Overpass JSON elements."""
    polygons = []
    for el in elements:
        if el.get("type") == "way":
            geom = el.get("geometry", [])
            if len(geom) >= 4:
                try:
                    coords = [(node["lon"], node["lat"]) for node in geom]
                    poly = Polygon(coords)
                    if poly.is_valid and poly.area > 0:
                        polygons.append(poly)
                except Exception:
                    continue
        elif el.get("type") == "relation":
            for member in el.get("members", []):
                if member.get("role") == "outer" and member.get("type") == "way":
                    member_geom = member.get("geometry", [])
                    if len(member_geom) >= 4:
                        try:
                            coords = [(n["lon"], n["lat"]) for n in member_geom]
                            poly = Polygon(coords)
                            if poly.is_valid and poly.area > 0:
                                polygons.append(poly)
                        except Exception:
                            continue
    return polygons


def _fetch_cemetery_polygons(south: float, west: float, north: float, east: float) -> list[Polygon]:
    """Fetch OSM cemetery/graveyard areas so buildings inside them can be excluded."""
    query = f"""
    [out:json][timeout:15];
    (
      way["landuse"="cemetery"]({south},{west},{north},{east});
      relation["landuse"="cemetery"]({south},{west},{north},{east});
      way["amenity"="grave_yard"]({south},{west},{north},{east});
    );
    out geom;
    """
    data = query_overpass(query, timeout=20.0, query_label="cemeteries")
    return _parse_polygons_from_elements(data.get("elements", []))


def fetch_building_polygons(west: float, south: float, east: float, north: float) -> list[Polygon]:
    """Fetch OSM building footprints for a bounding box via Overpass API.
    Returns a list of Shapely Polygons representing building outlines.
    Buildings inside cemeteries/graveyards are excluded.
    """
    query = f"""
    [out:json][timeout:30];
    (
      way["building"]({south},{west},{north},{east});
    );
    out geom;
    """
    data = query_overpass(query, timeout=45.0, query_label="buildings")
    elements = data.get("elements", [])
    if not elements:
        log.warning("overpass_no_buildings_returned", bbox=f"{west},{south},{east},{north}")
        return []
    all_buildings = _parse_polygons_from_elements(elements)
    # Exclude buildings that sit inside cemetery/graveyard areas
    cemetery_polys = _fetch_cemetery_polygons(south, west, north, east)
    if cemetery_polys:
        from shapely.ops import unary_union
        cemetery_mask = prep(unary_union(cemetery_polys))
        filtered = [b for b in all_buildings if not cemetery_mask.contains(b.centroid)]
        excluded = len(all_buildings) - len(filtered)
        if excluded:
            log.info("cemetery_buildings_excluded", excluded=excluded, total_buildings=len(all_buildings))
        all_buildings = filtered
    log.info("buildings_fetched", count=len(all_buildings), bbox=f"{west},{south},{east},{north}")
    return all_buildings


def filter_candidates_by_buildings(
    candidates: list,
    west: float, south: float, east: float, north: float,
) -> list:
    """Remove candidates whose centroid falls inside an OSM building footprint.

    Candidates are expected to have a .geometry attribute (Shapely Point in WGS84).
    """
    buildings = fetch_building_polygons(west, south, east, north)
    if not buildings:
        return candidates

    # Merge all buildings into a single MultiPolygon for fast lookup
    merged = MultiPolygon(buildings)
    prepared = prep(merged)

    original_count = len(candidates)
    filtered = []
    removed = 0

    for c in candidates:
        # Candidate geometry is in the raster's CRS (UTM), not WGS84 yet.
        # We need to check in WGS84. The candidates at this point still have
        # UTM coordinates. We'll accept lon/lat points too.
        point = c.geometry
        if prepared.contains(point):
            removed += 1
        else:
            filtered.append(c)

    log.info(
        "building_filter_applied",
        original=original_count,
        removed=removed,
        remaining=len(filtered),
        buildings=len(buildings),
    )
    return filtered
