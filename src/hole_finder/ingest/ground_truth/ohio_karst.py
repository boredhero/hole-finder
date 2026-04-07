"""Ohio EPA Karst Features loader — query ArcGIS Online feature layer.

Source: Ohio EPA GIS Portal
Access: REST API query (no download needed, paginated results).
"""

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point, shape
from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from hole_finder.utils.logging import log

# Ohio EPA karst feature service URL
OHIO_KARST_URL = (
    "https://services.arcgis.com/ULBthCfq5kAoGGkN/ArcGIS/rest/services/"
    "Known_and_Indicated_Karst_Locations/FeatureServer/0/query"
)


async def load_ohio_karst(session: AsyncSession, data_dir: str) -> int:
    """Load Ohio karst features from ArcGIS REST API."""
    count = 0
    offset = 0
    batch_size = 1000
    batch = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "outSR": "4326",
                "returnGeometry": "true",
                "f": "geojson",
                "resultRecordCount": batch_size,
                "resultOffset": offset,
            }

            try:
                response = await client.get(OHIO_KARST_URL, params=params)
                response.raise_for_status()
                data = response.json()
            except (httpx.HTTPError, Exception) as e:
                log.warning("ohio_karst_query_failed", error=str(e), offset=offset)
                break

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                props = feature.get("properties", {})
                geom_data = feature.get("geometry")
                if not geom_data:
                    continue

                try:
                    geom = shape(geom_data)
                except Exception as e:
                    log.debug("geom_parse_failed", source="ohio_karst", error=str(e))
                    continue

                if geom.geom_type != "Point":
                    geom = geom.centroid

                raw_type = str(props.get("TYPE", "") or props.get("Feature_Type", "")).upper()
                if "CAVE" in raw_type:
                    ft = FeatureType.CAVE_ENTRANCE
                elif "SINK" in raw_type:
                    ft = FeatureType.SINKHOLE
                elif "SPRING" in raw_type:
                    ft = FeatureType.SPRING
                else:
                    ft = FeatureType.UNKNOWN

                site = GroundTruthSite(
                    name=str(props.get("NAME", "") or f"OH-Karst-{count}"),
                    feature_type=ft,
                    geometry=from_shape(Point(geom.x, geom.y), srid=4326),
                    source=GroundTruthSource.OHIO_EPA,
                    source_id=str(props.get("OBJECTID", "")),
                    metadata_={
                        "county": str(props.get("COUNTY", "")),
                        "raw_type": raw_type,
                    },
                )
                batch.append(site)
                count += 1

                if len(batch) >= 500:
                    session.add_all(batch)
                    await session.flush()
                    batch.clear()

            offset += len(features)
            if len(features) < batch_size:
                break

    if batch:
        session.add_all(batch)
        await session.flush()

    await session.commit()
    log.info("ohio_karst_loaded", count=count)
    return count
