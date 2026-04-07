"""Maryland karst features loader — Maryland Geological Survey.

Source: Maryland Geological Survey (MGS) karst feature data.
53+ caves, 2100+ karst features in Hagerstown Valley and western MD.
"""

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point, shape
from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from hole_finder.utils.logging import log

# USGS MRDS REST endpoint — filter to Maryland
MRDS_URL = "https://mrdata.usgs.gov/mrds/wfs"


async def load_md_karst(session: AsyncSession, data_dir: str) -> int:
    """Load Maryland karst and mine features from USGS MRDS.

    Filters to Maryland for caves, mines, and karst features.
    """
    count = 0
    batch = []

    params = {
        "service": "WFS",
        "version": "1.1.0",
        "request": "GetFeature",
        "typeName": "mrds",
        "outputFormat": "application/json",
        "CQL_FILTER": "state_name='Maryland'",
        "maxFeatures": "5000",
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            response = await client.get(MRDS_URL, params=params)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, Exception) as e:
            log.warning("md_mrds_query_failed", error=str(e))
            return 0

        features = data.get("features", [])
        log.info("md_mrds_results", count=len(features))

        for feature in features:
            props = feature.get("properties", {})
            geom_data = feature.get("geometry")
            if not geom_data:
                continue

            try:
                geom = shape(geom_data)
            except Exception as e:
                log.debug("geom_parse_failed", source="md_karst", error=str(e))
                continue

            if geom.geom_type != "Point":
                geom = geom.centroid

            commodity = str(props.get("commod1", "") or "").upper()
            site_type = str(props.get("site_type", "") or "").upper()

            if "COAL" in commodity:
                ft = FeatureType.MINE_PORTAL
            elif "CAVE" in site_type:
                ft = FeatureType.CAVE_ENTRANCE
            elif "LIMESTONE" in commodity or "DOLOMITE" in commodity:
                ft = FeatureType.SINKHOLE
            else:
                ft = FeatureType.MINE_PORTAL

            site = GroundTruthSite(
                name=str(props.get("site_name", "") or f"MD-{count}"),
                feature_type=ft,
                geometry=from_shape(Point(geom.x, geom.y), srid=4326),
                source=GroundTruthSource.MD_KARST_SURVEY,
                source_id=str(props.get("dep_id", "") or props.get("rec_id", "")),
                metadata_={
                    "commodity": commodity,
                    "county": str(props.get("county", "")),
                    "site_type": site_type,
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
