"""PASDA Karst Features loader — 111,000+ karst feature points across 14 PA counties.

Download from: https://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=3073
Format: Shapefile with point geometries.
"""

from pathlib import Path

import geopandas as gpd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from magic_eyes.utils.logging import log


# Mapping from PASDA feature type codes to our enum
PASDA_TYPE_MAP = {
    "SINKHOLE": FeatureType.SINKHOLE,
    "SURFACE DEPRESSION": FeatureType.DEPRESSION,
    "CAVE": FeatureType.CAVE_ENTRANCE,
    "CAVE ENTRANCE": FeatureType.CAVE_ENTRANCE,
    "SPRING": FeatureType.SPRING,
    "MINE": FeatureType.MINE_PORTAL,
    "SURFACE MINE": FeatureType.MINE_PORTAL,
    "COLLAPSE": FeatureType.COLLAPSE_PIT,
}


async def load_pasda_karst(session: AsyncSession, data_dir: str) -> int:
    """Load PASDA karst features shapefile into ground_truth_sites table.

    Args:
        session: async DB session
        data_dir: directory containing downloaded shapefiles

    Returns:
        Number of features loaded
    """
    shapefile_path = Path(data_dir) / "ground_truth" / "pasda_karst"

    # Look for .shp file in the directory
    shp_files = list(shapefile_path.glob("*.shp"))
    if not shp_files:
        log.warning("pasda_karst_not_found", path=str(shapefile_path))
        return 0

    shp_file = shp_files[0]
    log.info("loading_pasda_karst", file=str(shp_file))

    gdf = gpd.read_file(shp_file)

    # Reproject to WGS84 if needed
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    count = 0
    batch = []

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        # Get centroid if not a point
        if geom.geom_type != "Point":
            geom = geom.centroid

        # Map feature type
        raw_type = str(row.get("FEAT_TYPE", "") or row.get("TYPE", "") or "").upper().strip()
        feature_type = PASDA_TYPE_MAP.get(raw_type, FeatureType.UNKNOWN)

        site = GroundTruthSite(
            name=str(row.get("NAME", "") or row.get("FEAT_NAME", "") or f"PASDA-{count}"),
            feature_type=feature_type,
            geometry=from_shape(Point(geom.x, geom.y), srid=4326),
            source=GroundTruthSource.PASDA_KARST,
            source_id=str(row.get("OBJECTID", "") or row.get("FID", "")),
            metadata_={
                "county": str(row.get("COUNTY", "")),
                "quad": str(row.get("QUAD_NAME", "")),
                "raw_type": raw_type,
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
