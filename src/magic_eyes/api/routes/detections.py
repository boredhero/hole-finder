"""Detection CRUD and spatial query endpoints."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from geoalchemy2.shape import to_shape
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.api.deps import get_db
from magic_eyes.api.schemas import (
    DetectionCollection,
    DetectionDetail,
    DetectionFeature,
    DetectionProperties,
)
from magic_eyes.db.models import Detection, FeatureType, PassResult, ValidationEvent

router = APIRouter(tags=["detections"])


def _detection_to_feature(d: Detection) -> DetectionFeature:
    """Convert a Detection ORM object to a GeoJSON Feature."""
    try:
        point = to_shape(d.geometry)
        geom = {"type": "Point", "coordinates": [point.x, point.y]}
    except Exception:
        geom = {"type": "Point", "coordinates": [0, 0]}

    return DetectionFeature(
        id=str(d.id),
        geometry=geom,
        properties=DetectionProperties(
            feature_type=d.feature_type.value if d.feature_type else None,
            confidence=d.confidence or 0.0,
            depth_m=d.depth_m,
            area_m2=d.area_m2,
            circularity=d.circularity,
            wall_slope_deg=d.wall_slope_deg,
            source_passes=d.source_passes,
            morphometrics=d.morphometrics,
            validated=d.validated,
            validation_notes=d.validation_notes,
        ),
    )


@router.get("/detections", response_model=DetectionCollection)
async def list_detections(
    west: float = Query(..., description="Bounding box west longitude"),
    south: float = Query(..., description="Bounding box south latitude"),
    east: float = Query(..., description="Bounding box east longitude"),
    north: float = Query(..., description="Bounding box north latitude"),
    feature_type: list[str] | None = Query(None, description="Filter by feature type"),
    source_pass: str | None = Query(None, description="Filter by source detection pass name"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    validated: bool | None = Query(None, description="Filter by validation status"),
    limit: int = Query(10000, le=50000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Query detections within a bounding box, returned as GeoJSON FeatureCollection."""
    from geoalchemy2.functions import ST_MakeEnvelope

    envelope = ST_MakeEnvelope(west, south, east, north, 4326)
    stmt = (
        select(Detection)
        .where(Detection.geometry.ST_Within(envelope))
        .where(Detection.confidence >= min_confidence)
    )

    if feature_type:
        ft_enums = [FeatureType(ft) for ft in feature_type if ft in FeatureType.__members__]
        if ft_enums:
            stmt = stmt.where(Detection.feature_type.in_(ft_enums))

    if source_pass:
        stmt = stmt.where(Detection.source_passes.contains([source_pass]))

    if validated is not None:
        stmt = stmt.where(Detection.validated == validated)

    # Count total before pagination
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = (await db.execute(count_stmt)).scalar_one()

    stmt = stmt.order_by(Detection.confidence.desc()).offset(offset).limit(limit)
    result = await db.execute(stmt)
    detections = result.scalars().all()

    features = [_detection_to_feature(d) for d in detections]
    return DetectionCollection(features=features, total_count=total)


@router.get("/detections/{detection_id}", response_model=DetectionDetail)
async def get_detection(
    detection_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get detailed info for a single detection."""
    detection = await db.get(Detection, detection_id)
    if not detection:
        raise HTTPException(status_code=404, detail="Detection not found")

    # Load pass results
    pr_stmt = select(PassResult).where(PassResult.detection_id == detection_id)
    pass_results = (await db.execute(pr_stmt)).scalars().all()

    # Load validation events
    ve_stmt = select(ValidationEvent).where(ValidationEvent.detection_id == detection_id)
    val_events = (await db.execute(ve_stmt)).scalars().all()

    return DetectionDetail(
        id=str(detection.id),
        feature_type=detection.feature_type.value if detection.feature_type else None,
        confidence=detection.confidence or 0.0,
        depth_m=detection.depth_m,
        area_m2=detection.area_m2,
        circularity=detection.circularity,
        morphometrics=detection.morphometrics,
        source_passes=detection.source_passes,
        validated=detection.validated,
        validation_notes=detection.validation_notes,
        pass_results=[
            {"pass_name": pr.pass_name, "raw_score": pr.raw_score, "parameters": pr.parameters}
            for pr in pass_results
        ],
        validation_events=[
            {"verdict": ve.verdict.value, "notes": ve.notes, "created_at": str(ve.created_at)}
            for ve in val_events
        ],
        created_at=detection.created_at,
    )
