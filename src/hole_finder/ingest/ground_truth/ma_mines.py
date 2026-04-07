"""Massachusetts mine and cave features loader — USGS MRDS.

Source: USGS Mineral Resources Data System
160+ mines in Berkshire County marble belt, pyrite/mica mines.
"""

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point, shape
from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from hole_finder.utils.logging import log

MRDS_URL = "https://mrdata.usgs.gov/mrds/wfs"


async def load_ma_mines(session: AsyncSession, data_dir: str) -> int:
    """Load Massachusetts mine features from USGS MRDS."""
    count = 0
    batch = []

    params = {
        "service": "WFS",
        "version": "1.1.0",
        "request": "GetFeature",
        "typeName": "mrds",
        "outputFormat": "application/json",
        "CQL_FILTER": "state_name='Massachusetts'",
        "maxFeatures": "5000",
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            response = await client.get(MRDS_URL, params=params)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, Exception) as e:
            log.warning("ma_mrds_query_failed", error=str(e))
            return 0

        features = data.get("features", [])
        log.info("ma_mrds_results", count=len(features))

        for feature in features:
            props = feature.get("properties", {})
            geom_data = feature.get("geometry")
            if not geom_data:
                continue

            try:
                geom = shape(geom_data)
            except Exception as e:
                log.debug("geom_parse_failed", source="ma_mines", error=str(e))
                continue

            if geom.geom_type != "Point":
                geom = geom.centroid

            commodity = str(props.get("commod1", "") or "").upper()

            if "MARBLE" in commodity or "LIMESTONE" in commodity:
                ft = FeatureType.CAVE_ENTRANCE
            else:
                ft = FeatureType.MINE_PORTAL

            site = GroundTruthSite(
                name=str(props.get("site_name", "") or f"MA-{count}"),
                feature_type=ft,
                geometry=from_shape(Point(geom.x, geom.y), srid=4326),
                source=GroundTruthSource.MA_USGS_MINES,
                source_id=str(props.get("dep_id", "") or props.get("rec_id", "")),
                metadata_={
                    "commodity": commodity,
                    "county": str(props.get("county", "")),
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
