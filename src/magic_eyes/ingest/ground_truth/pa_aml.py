"""PA Abandoned Mine Land (AML) Inventory loader — 11,249 mine sites.

Download from: https://www.pasda.psu.edu/ (search "AML Inventory")
or PA DEP GIS: https://gis.dep.pa.gov/
Format: Shapefile with point/polygon geometries.
"""

from pathlib import Path

import geopandas as gpd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from magic_eyes.utils.logging import log


async def load_pa_aml(session: AsyncSession, data_dir: str) -> int:
    """Load PA AML inventory into ground_truth_sites."""
    shapefile_path = Path(data_dir) / "ground_truth" / "pa_aml"

    shp_files = list(shapefile_path.glob("*.shp"))
    if not shp_files:
        log.warning("pa_aml_not_found", path=str(shapefile_path))
        return 0

    gdf = gpd.read_file(shp_files[0])
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
            name=str(row.get("MINE_NAME", "") or row.get("SITE_NAME", "") or f"AML-{count}"),
            feature_type=FeatureType.MINE_PORTAL,
            geometry=from_shape(Point(geom.x, geom.y), srid=4326),
            source=GroundTruthSource.PA_AML,
            source_id=str(row.get("AML_ID", "") or row.get("OBJECTID", "")),
            metadata_={
                "county": str(row.get("COUNTY", "")),
                "mine_type": str(row.get("MINE_TYPE", "")),
                "status": str(row.get("STATUS", "")),
                "hazard_class": str(row.get("HAZARD", "")),
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
