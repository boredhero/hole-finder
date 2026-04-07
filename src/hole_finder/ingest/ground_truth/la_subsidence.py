"""Louisiana subsidence and salt dome collapse loader.

Source: USGS MRDS for mineral/salt sites + known salt dome monitoring locations.
Louisiana's primary hazard is salt dome collapse (evaporite dissolution),
not limestone karst.
"""

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point, shape
from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.db.models import FeatureType, GroundTruthSite, GroundTruthSource
from hole_finder.utils.logging import log

MRDS_URL = "https://mrdata.usgs.gov/mrds/wfs"

# Known salt dome collapse / subsidence sites with precise coordinates
KNOWN_LA_SITES = [
    {
        "name": "Bayou Corne Sinkhole",
        "lat": 30.0008,
        "lon": -91.1117,
        "type": FeatureType.SALT_DOME_COLLAPSE,
        "notes": "37-acre active salt dome collapse, Napoleonville Dome",
    },
    {
        "name": "Lake Peigneur",
        "lat": 30.0006,
        "lon": -91.9833,
        "type": FeatureType.SALT_DOME_COLLAPSE,
        "notes": "1980 drilling collapse into Jefferson Island salt mine",
    },
    {
        "name": "Grand Bayou Subsidence",
        "lat": 29.4500,
        "lon": -89.9500,
        "type": FeatureType.SALT_DOME_COLLAPSE,
        "notes": "Coastal subsidence area, Plaquemines Parish",
    },
]


async def load_la_subsidence(session: AsyncSession, data_dir: str) -> int:
    """Load Louisiana salt dome and subsidence features."""
    count = 0
    batch = []

    # First: insert known salt dome collapse sites
    for site_data in KNOWN_LA_SITES:
        site = GroundTruthSite(
            name=site_data["name"],
            feature_type=site_data["type"],
            geometry=from_shape(Point(site_data["lon"], site_data["lat"]), srid=4326),
            source=GroundTruthSource.LA_SUBSIDENCE,
            source_id=f"la_known_{site_data['name'].lower().replace(' ', '_')}",
            metadata_={"notes": site_data["notes"]},
        )
        batch.append(site)
        count += 1

    # Second: query USGS MRDS for Louisiana mineral resources (salt, sulfur)
    params = {
        "service": "WFS",
        "version": "1.1.0",
        "request": "GetFeature",
        "typeName": "mrds",
        "outputFormat": "application/json",
        "CQL_FILTER": "state_name='Louisiana'",
        "maxFeatures": "5000",
    }

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            response = await client.get(MRDS_URL, params=params)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, Exception) as e:
            log.warning("la_mrds_query_failed", error=str(e))
            # Still commit the known sites
            if batch:
                session.add_all(batch)
                await session.flush()
            await session.commit()
            return count

        features = data.get("features", [])
        log.info("la_mrds_results", count=len(features))

        for feature in features:
            props = feature.get("properties", {})
            geom_data = feature.get("geometry")
            if not geom_data:
                continue

            try:
                geom = shape(geom_data)
            except Exception as e:
                log.debug("geom_parse_failed", source="la_subsidence", error=str(e))
                continue

            if geom.geom_type != "Point":
                geom = geom.centroid

            commodity = str(props.get("commod1", "") or "").upper()

            if "SALT" in commodity or "BRINE" in commodity:
                ft = FeatureType.SALT_DOME_COLLAPSE
            elif "SULFUR" in commodity:
                ft = FeatureType.SALT_DOME_COLLAPSE
            else:
                ft = FeatureType.MINE_PORTAL

            site = GroundTruthSite(
                name=str(props.get("site_name", "") or f"LA-{count}"),
                feature_type=ft,
                geometry=from_shape(Point(geom.x, geom.y), srid=4326),
                source=GroundTruthSource.LA_SUBSIDENCE,
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
