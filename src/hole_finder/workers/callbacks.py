"""Progress reporting callbacks for Celery tasks."""

import asyncio
from datetime import UTC, datetime

from hole_finder.db.engine import async_session_factory
from hole_finder.db.models import Job
from hole_finder.utils.log_manager import log


async def _update_progress(job_id: str, percent: float, message: str) -> None:
    """Update job progress in the database."""
    from uuid import UUID
    async with async_session_factory() as session:
        job = await session.get(Job, UUID(job_id))
        if job:
            job.progress = percent
            log.debug("job_progress_updated", job_id=job_id[:8], percent=round(percent, 1), message=message)
            if percent >= 100:
                from hole_finder.db.models import JobStatus
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now(UTC)
                log.info("job_marked_completed", job_id=job_id[:8])
            await session.commit()
        else:
            log.warning("job_progress_update_job_not_found", job_id=job_id[:8], percent=percent)


def update_job_progress(job_id: str, percent: float, message: str = "") -> None:
    """Update job progress — sync wrapper for use in Celery tasks."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            log.debug("job_progress_async_future", job_id=job_id[:8], percent=round(percent, 1))
            asyncio.ensure_future(_update_progress(job_id, percent, message))
        else:
            asyncio.run(_update_progress(job_id, percent, message))
    except RuntimeError:
        log.debug("job_progress_no_event_loop_fallback", job_id=job_id[:8], percent=round(percent, 1))
        asyncio.run(_update_progress(job_id, percent, message))
