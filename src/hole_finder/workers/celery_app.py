"""Celery application factory."""

# Disable PROJ network BEFORE any pyproj/rasterio import — prevents grid-based
# NAD83→WGS84 pipelines that return inf in forked Celery workers.
# The simple geocentric transform (~4m accuracy) is more than sufficient for LiDAR.
import pyproj.network
pyproj.network.set_network_enabled(False)

from celery import Celery

from hole_finder.config import settings

app = Celery("hole_finder")
app.config_from_object(
    {
        "broker_url": settings.redis_url,
        "result_backend": settings.redis_url.replace("/0", "/1"),
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "task_track_started": True,
        "task_acks_late": True,
        "worker_prefetch_multiplier": 1,
        "task_routes": {
            "hole_finder.workers.tasks.download_tile": {"queue": "ingest"},
            "hole_finder.workers.tasks.process_tile": {"queue": "process"},
            "hole_finder.workers.tasks.run_detection": {"queue": "detect"},
            "hole_finder.workers.tasks.run_ml_pass": {"queue": "gpu"},
        },
        "task_time_limit": 3600,
        "task_soft_time_limit": 3000,
    }
)

app.autodiscover_tasks(["hole_finder.workers"])

# Periodic tasks (Celery Beat)
app.conf.beat_schedule = {
    "storage-eviction": {
        "task": "hole_finder.workers.tasks.run_storage_eviction",
        "schedule": 86400.0,  # Daily
    },
}
