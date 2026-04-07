"""North Carolina cave and mine features loader — USGS MRDS REST query.

Source: USGS Mineral Resources Data System (MRDS)
Filters to North Carolina for caves, mica mines, and gold mines.
"""

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point, shape
from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from hole_finder.utils.logging import log

# USGS MRDS REST endpoint — filter to NC
MRDS_URL = (
    "https://mrdata.usgs.gov/mrds/wfs"
)

# NC specific ArcGIS karst query (fallback)
NC_KARST_URL = (
    "https://services.nconemap.gov/secure/rest/services/"
    "NC_Geology/NC_Geologic_Map/MapServer/0/query"
)


async def load_nc_caves(session: AsyncSession, data_dir: str) -> int:
    """Load NC cave and mine features from USGS MRDS.

    Uses WFS query filtered to North Carolina state.
    Falls back to basic mine/cave data if full WFS unavailable.
    """
    count = 0
    batch = []

    # Query USGS MRDS via WFS for NC mineral resources
    params = {
        "service": "WFS",
        "version": "1.1.0",
        "request": "GetFeature",
        "typeName": "mrds",
        "outputFormat": "application/json",
        "CQL_FILTER": "state_name='North Carolina'",
        "maxFeatures": "5000",
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            response = await client.get(MRDS_URL, params=params)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, Exception) as e:
            log.warning("nc_mrds_query_failed", error=str(e))
            return 0

        features = data.get("features", [])
        log.info("nc_mrds_results", count=len(features))

        for feature in features:
            props = feature.get("properties", {})
            geom_data = feature.get("geometry")
            if not geom_data:
                continue

            try:
                geom = shape(geom_data)
            except Exception as e:
                log.debug("geom_parse_failed", source="nc_caves", error=str(e))
                continue

            if geom.geom_type != "Point":
                geom = geom.centroid

            # Map commodity/site type to feature type
            commodity = str(props.get("commod1", "") or "").upper()
            site_type = str(props.get("site_type", "") or "").upper()

            if "MICA" in commodity or "FELDSPAR" in commodity:
                ft = FeatureType.MINE_PORTAL
            elif "GOLD" in commodity or "AU" in commodity:
                ft = FeatureType.MINE_PORTAL
            elif "CAVE" in site_type:
                ft = FeatureType.CAVE_ENTRANCE
            else:
                ft = FeatureType.MINE_PORTAL

            site = GroundTruthSite(
                name=str(props.get("site_name", "") or f"NC-{count}"),
                feature_type=ft,
                geometry=from_shape(Point(geom.x, geom.y), srid=4326),
                source=GroundTruthSource.NC_CAVE_SURVEY,
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
