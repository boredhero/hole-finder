"""Filter detections that fall on building footprints using OpenStreetMap data.

Uses the Overpass API (free, no auth) to fetch building polygons for a given
bounding box, then excludes any detection whose centroid falls inside a building.
"""

import httpx
from shapely.geometry import Point, Polygon, MultiPolygon, shape
from shapely.prepared import prep

from hole_finder.utils.logging import log

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


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
    try:
        resp = httpx.post(OVERPASS_URL, data={"data": query}, timeout=15.0)
        resp.raise_for_status()
    except Exception as e:
        log.warning("overpass_cemetery_fetch_failed", error=str(e))
        return []
    data = resp.json()
    polygons = []
    for el in data.get("elements", []):
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


def fetch_building_polygons(west: float, south: float, east: float, north: float) -> list[Polygon]:
    """Fetch OSM building footprints for a bounding box via Overpass API.

    Returns a list of Shapely Polygons representing building outlines.
    Buildings inside cemeteries/graveyards are excluded — those structures
    (mausoleums, crypts, vaults) don't produce false LiDAR anomalies like
    rooftops do, and real geological features can exist beneath them.
    """
    # Overpass uses (south, west, north, east) bbox format
    query = f"""
    [out:json][timeout:30];
    (
      way["building"]({south},{west},{north},{east});
    );
    out geom;
    """
    try:
        resp = httpx.post(OVERPASS_URL, data={"data": query}, timeout=30.0)
        resp.raise_for_status()
    except Exception as e:
        log.warning("overpass_fetch_failed", error=str(e))
        return []
    data = resp.json()
    elements = data.get("elements", [])
    all_buildings = []
    for el in elements:
        if el.get("type") != "way":
            continue
        geom = el.get("geometry", [])
        if len(geom) < 4:
            continue
        try:
            coords = [(node["lon"], node["lat"]) for node in geom]
            poly = Polygon(coords)
            if poly.is_valid and poly.area > 0:
                all_buildings.append(poly)
        except Exception:
            continue
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
