"""USGS National Karst Map loader — karst areas across entire US.

Source: USGS Open-File Report 2014-1156
Download: https://pubs.usgs.gov/of/2014/1156/
Format: Shapefile (269MB, polygon geometries for karst-prone areas).

We filter to target states (PA, WV, OH, NY) and extract centroids.
"""

from pathlib import Path

import geopandas as gpd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from magic_eyes.utils.logging import log

TARGET_STATES = {"PA", "WV", "OH", "NY", "Pennsylvania", "West Virginia", "Ohio", "New York"}


async def load_usgs_national(session: AsyncSession, data_dir: str) -> int:
    """Load USGS national karst map, filtered to target states."""
    base_path = Path(data_dir) / "ground_truth" / "usgs_national"

    shp_files = list(base_path.glob("*.shp"))
    if not shp_files:
        log.warning("usgs_national_not_found", path=str(base_path))
        return 0

    gdf = gpd.read_file(shp_files[0])

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Filter to target states if state column exists
    state_col = None
    for col in ("STATE", "State", "state", "STUSPS", "NAME"):
        if col in gdf.columns:
            state_col = col
            break

    if state_col:
        gdf = gdf[gdf[state_col].isin(TARGET_STATES)]

    count = 0
    batch = []

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        centroid = geom.centroid

        site = GroundTruthSite(
            name=str(row.get("UNIT_NAME", "") or row.get("MAP_UNIT", "") or f"USGS-Karst-{count}"),
            feature_type=FeatureType.UNKNOWN,  # karst area, not specific feature
            geometry=from_shape(Point(centroid.x, centroid.y), srid=4326),
            source=GroundTruthSource.USGS_NATIONAL,
            source_id=str(row.get("OBJECTID", "") or row.get("FID", "")),
            metadata_={
                "rock_type": str(row.get("ROCK_TYPE", "")),
                "state": str(row.get(state_col, "")) if state_col else "",
                "area_km2": float(geom.area) if geom.area else 0,
            },
        )
        batch.append(site)
        count += 1

        if len(batch) >= 500:
            session.add_all(batch)
            await session.flush()
            batch.clear()

    if batch:
        session.add_all(batch)
        await session.flush()

    await session.commit()
    return count
