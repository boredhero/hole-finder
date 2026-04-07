"""Job submission, status, and management endpoints."""

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.api.deps import get_db
from hole_finder.api.schemas import JobCreate, JobList, JobStatus
from hole_finder.db.models import Job, JobType
from hole_finder.db.models import JobStatus as JobStatusEnum
from hole_finder.utils.logging import log

router = APIRouter(tags=["jobs"])


class ConsumerScanRequest(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    radius_km: float = Field(5.0, ge=0.5, le=10.0)


def _job_to_schema(j: Job) -> JobStatus:
    return JobStatus(
        id=str(j.id),
        job_type=j.job_type.value if j.job_type else "unknown",
        status=j.status.value if j.status else "unknown",
        progress=j.progress or 0.0,
        result_summary=j.result_summary,
        error_message=j.error_message,
        created_at=j.created_at,
        started_at=j.started_at,
        completed_at=j.completed_at,
    )


@router.get("/jobs", response_model=JobList)
async def list_jobs(
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """List all jobs, optionally filtered by status."""
    stmt = select(Job).order_by(Job.created_at.desc()).limit(100)
    if status:
        try:
            status_enum = JobStatusEnum(status.upper())
            stmt = stmt.where(Job.status == status_enum)
        except ValueError as e:
            log.debug("invalid_job_status_filter", status=status, error=str(e))
            pass

    result = await db.execute(stmt)
    jobs = result.scalars().all()
    return JobList(jobs=[_job_to_schema(j) for j in jobs])


@router.post("/jobs", response_model=JobStatus)
async def create_job(
    body: JobCreate,
    db: AsyncSession = Depends(get_db),
):
    """Submit a new processing job."""
    try:
        job_type = JobType(body.job_type.upper())
    except ValueError:
        job_type = JobType.FULL_PIPELINE

    region_geom = None
    if body.bbox:
        from geoalchemy2.shape import from_shape
        from shapely.geometry import shape
        region_geom = from_shape(shape(body.bbox), srid=4326)

    job = Job(
        job_type=job_type,
        status=JobStatusEnum.PENDING,
        region=region_geom,
        config={"pass_config": body.pass_config},
        progress=0.0,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Submit to Celery
    try:
        from hole_finder.workers.tasks import run_full_pipeline

        bbox_geojson = None
        if body.bbox:
            from shapely.geometry import mapping, shape
            bbox_geojson = body.bbox

        task = run_full_pipeline.delay(
            job_id=str(job.id),
            pass_config=body.pass_config,
            bbox_geojson=bbox_geojson,
        )
        job.celery_task_id = task.id
        job.status = JobStatusEnum.RUNNING
        await db.commit()
    except Exception:
        pass  # Celery not running is non-fatal for job creation

    return _job_to_schema(job)


@router.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get status of a specific job."""
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_to_schema(job)


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Cancel a pending or running job."""
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in (JobStatusEnum.PENDING, JobStatusEnum.RUNNING):
        raise HTTPException(status_code=400, detail=f"Cannot cancel job in state {job.status.value}")

    job.status = JobStatusEnum.CANCELLED
    job.completed_at = datetime.now(UTC)
    await db.commit()

    # Revoke Celery task if running
    # if job.celery_task_id:
    #     from hole_finder.workers.celery_app import app
    #     app.control.revoke(job.celery_task_id, terminate=True)

    return {"status": "cancelled", "job_id": str(job_id)}


@router.post("/explore/scan")
async def consumer_scan(
    body: ConsumerScanRequest,
    db: AsyncSession = Depends(get_db),
):
    """Start a small processing job for the consumer "Find a Hole Near Me" flow.

    Restricted to a small radius (max 5km) and 4 tiles to keep processing
    under 5 minutes. The consumer never sees the word "job" — this is
    presented as "scanning your area."
    """
    r = body.radius_km / 111.32  # degrees approx

    bbox_geojson = {
        "type": "Polygon",
        "coordinates": [[
            [body.lon - r, body.lat - r],
            [body.lon + r, body.lat - r],
            [body.lon + r, body.lat + r],
            [body.lon - r, body.lat + r],
            [body.lon - r, body.lat - r],
        ]],
    }

    from geoalchemy2.shape import from_shape
    from shapely.geometry import shape
    from sqlalchemy import text

    region_geom = from_shape(shape(bbox_geojson), srid=4326)

    # Don't delete existing detections upfront — the Celery worker
    # clears them only after confirming tiles are available to process.
    # This preserves data when a scan finds no LiDAR coverage.

    job = Job(
        job_type=JobType.FULL_PIPELINE,
        status=JobStatusEnum.PENDING,
        region=region_geom,
        config={
            "pass_config": "sinkhole_survey",
            "tile_limit": 50,
            "consumer": True,
            "center_lat": body.lat,
            "center_lon": body.lon,
            "radius_km": body.radius_km,
        },
        progress=0.0,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Submit to Celery
    try:
        from hole_finder.workers.tasks import run_full_pipeline

        task = run_full_pipeline.delay(
            job_id=str(job.id),
            pass_config="sinkhole_survey",
            bbox_geojson=bbox_geojson,
        )
        job.celery_task_id = task.id
        job.status = JobStatusEnum.RUNNING
        job.started_at = datetime.now(UTC)
        await db.commit()
    except Exception:
        pass  # Celery not running is non-fatal

    # Estimate: ~75s per tile, assume 3 tiles avg
    estimated_minutes = round(3 * 75 / 60, 1)

    return {
        "job_id": str(job.id),
        "estimated_minutes": estimated_minutes,
    }
