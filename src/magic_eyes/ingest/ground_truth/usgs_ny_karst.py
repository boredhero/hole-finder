"""USGS NY Karst Closed Depression Inventory — 5,023 features statewide.

Source: USGS Scientific Investigations Report 2020-5030
Download: https://www.sciencebase.gov/catalog/item/562a313ae4b011227bf1fe23
Format: Geodatabase or shapefile.
"""

from pathlib import Path

import geopandas as gpd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from magic_eyes.utils.logging import log


async def load_usgs_ny_karst(session: AsyncSession, data_dir: str) -> int:
    """Load USGS NY closed depression inventory."""
    base_path = Path(data_dir) / "ground_truth" / "usgs_ny_karst"

    # Try geodatabase first, then shapefile
    gdb_files = list(base_path.glob("*.gdb"))
    shp_files = list(base_path.glob("*.shp"))

    if gdb_files:
        gdf = gpd.read_file(gdb_files[0])
    elif shp_files:
        gdf = gpd.read_file(shp_files[0])
    else:
        log.warning("usgs_ny_not_found", path=str(base_path))
        return 0

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    count = 0
    batch = []

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        if geom.geom_type != "Point":
            geom = geom.centroid

        site = GroundTruthSite(
            name=str(row.get("NAME", "") or f"NY-Depression-{count}"),
            feature_type=FeatureType.DEPRESSION,
            geometry=from_shape(Point(geom.x, geom.y), srid=4326),
            source=GroundTruthSource.USGS_NY,
            source_id=str(row.get("OBJECTID", "") or row.get("FID", "")),
            metadata_={
                "source_method": str(row.get("SOURCE", "")),
                "county": str(row.get("COUNTY", "")),
                "geology": str(row.get("GEOLOGY", "")),
            },
        )
        batch.append(site)
        count += 1

        if len(batch) >= 1000:
            session.add_all(batch)
            await session.flush()
            batch.clear()

    if batch:
        session.add_all(batch)
        await session.flush()

    await session.commit()
    return count
