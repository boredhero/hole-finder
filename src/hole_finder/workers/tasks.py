"""Celery task definitions — includes full end-to-end pipeline orchestrator.

Thread/process safety notes:
- Each Celery task runs in its own worker process (prefork pool).
- PipelineProfiler is per-process, created fresh per task via new_profiler().
- numpy arrays are read-only shared across threads within a pass run.
- ProcessPoolExecutor (derivatives) spawns child processes that can't share
  the parent's profiler — they return timing data which is fed back.

asyncio + Celery note:
  Celery tasks are synchronous. We use asyncio.run() to call async DB/ingest
  code. Each asyncio.run() creates a NEW event loop, so we CANNOT use the
  module-level async engine (its connection pool is bound to a different loop).
  Instead, _async_session() creates a fresh engine+session per call.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import numpy as np
import rasterio

from shapely.ops import transform as shapely_transform

from hole_finder.config import settings
from hole_finder.utils.logging import log
from hole_finder.utils.perf import PipelineProfiler, get_profiler, new_profiler
from hole_finder.workers.celery_app import app


def _transform_outline(outline, transformer):
    """Transform an outline polygon from source CRS to WGS84."""
    if outline is None:
        return None
    try:
        return shapely_transform(lambda x, y: transformer.transform(x, y), outline)
    except Exception as e:
        log.warning("outline_transform_failed", error=str(e))
        return None


@asynccontextmanager
async def _async_session():
    """Create a one-shot async session with a fresh engine.

    Each asyncio.run() creates a new event loop. asyncpg connections are
    bound to the loop they were created on. So we MUST create a new engine
    per asyncio.run() call — reusing the module-level engine causes
    'Future attached to a different loop' errors.

    The engine is disposed after the session closes to avoid leaking
    connection pools across event loops.
    """
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(settings.database_url, echo=False, pool_size=2)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with factory() as session:
            yield session
    finally:
        await engine.dispose()


@app.task(bind=True, queue="ingest", max_retries=3)
def download_tile(self, source_name: str, tile_info_dict: dict, dest_dir: str):
    """Download a single LiDAR tile from the given source."""
    from shapely.geometry import shape

    from hole_finder.ingest.manager import get_source
    from hole_finder.ingest.sources.base import TileInfo

    self.update_state(state="PROGRESS", meta={"percent": 0, "message": "Starting download"})

    source = get_source(source_name)
    tile = TileInfo(
        source_id=tile_info_dict["source_id"],
        filename=tile_info_dict["filename"],
        url=tile_info_dict["url"],
        bbox=shape(tile_info_dict["bbox"]),
        crs=tile_info_dict.get("crs", 4326),
        file_size_bytes=tile_info_dict.get("file_size_bytes"),
        format=tile_info_dict.get("format", "laz"),
    )

    dest = Path(dest_dir)
    result_path = asyncio.run(source.download_tile(tile, dest))

    self.update_state(state="PROGRESS", meta={"percent": 100, "message": "Complete"})
    return str(result_path)


@app.task(bind=True, queue="process")
def process_tile(self, tile_path: str, output_dir: str | None = None):
    """Generate DEM and derivatives for a tile."""
    from hole_finder.processing.pipeline import ProcessingPipeline

    self.update_state(state="PROGRESS", meta={"percent": 0, "message": "Processing"})

    input_path = Path(tile_path)
    out_dir = Path(output_dir) if output_dir else settings.processed_dir

    pipeline = ProcessingPipeline(output_dir=out_dir)

    if input_path.suffix in (".laz", ".las"):
        result = pipeline.process_point_cloud(input_path)
    elif input_path.suffix in (".tif", ".tiff"):
        result = pipeline.process_dem_file(input_path)
    else:
        raise ValueError(f"Unsupported file type: {input_path.suffix}")

    self.update_state(state="PROGRESS", meta={"percent": 100, "message": "Complete"})
    return {
        "dem_path": str(result.dem_path),
        "derivative_paths": {k: str(v) for k, v in result.derivative_paths.items()},
        "resolution_m": result.resolution_m,
        "crs": result.crs,
    }


@app.task(bind=True, queue="detect")
def run_detection(self, dem_path: str, derivative_paths: dict, pass_config_name: str):
    """Run detection passes on a processed tile and store results in PostGIS."""
    import hole_finder.detection.passes  # register passes

    from geoalchemy2.shape import from_shape
    from pyproj import Transformer
    from shapely.geometry import Point

    from hole_finder.db.models import Detection
    from hole_finder.db.models import FeatureType as DBFeatureType
    from hole_finder.detection.runner import PassRunner

    profiler = new_profiler(f"run_detection:{Path(dem_path).stem}")
    self.update_state(state="PROGRESS", meta={"percent": 0, "message": "Running detection"})

    # Load DEM + derivatives
    with profiler.stage("load_rasters", parent="detection_io"):
        with rasterio.open(dem_path) as src:
            dem = src.read(1).astype(np.float32)
            transform = src.transform
            crs_code = src.crs.to_epsg() or 32617

        derivs = {}
        total_bytes = dem.nbytes
        for name, path in derivative_paths.items():
            with rasterio.open(path) as src:
                arr = src.read(1).astype(np.float32)
                derivs[name] = arr
                total_bytes += arr.nbytes

    log.info("rasters_loaded", dem_shape=list(dem.shape), derivatives=len(derivs),
             total_mb=round(total_bytes / 1e6, 1))

    # Run passes
    config_path = Path(f"/app/configs/passes/{pass_config_name}.toml")
    if not config_path.exists():
        config_path = settings.data_dir.parent / f"configs/passes/{pass_config_name}.toml"

    runner = PassRunner.from_toml(config_path)
    candidates = runner.run_on_array(dem, transform, crs_code, derivs)

    self.update_state(state="PROGRESS", meta={"percent": 70, "message": f"Storing {len(candidates)} detections"})

    # Filter and transform to WGS84
    transformer = Transformer.from_crs(f"EPSG:{crs_code}", "EPSG:4326", always_xy=True)
    ft_map = {
        "sinkhole": DBFeatureType.SINKHOLE,
        "cave_entrance": DBFeatureType.CAVE_ENTRANCE,
        "mine_portal": DBFeatureType.MINE_PORTAL,
        "depression": DBFeatureType.DEPRESSION,
        "collapse_pit": DBFeatureType.COLLAPSE_PIT,
        "unknown": DBFeatureType.UNKNOWN,
    }

    good = [c for c in candidates
            if c.score > 0.4
            and c.morphometrics.get("area_m2", 0) > 50
            and (c.morphometrics.get("depth_m", 0) or c.morphometrics.get("lrm_anomaly_m", 0)) < 100]

    async def _store():
        async with _async_session() as session:
            batch = []
            for c in good:
                lon, lat = transformer.transform(c.geometry.x, c.geometry.y)
                outline_wgs84 = _transform_outline(c.outline, transformer)
                det = Detection(
                    feature_type=ft_map.get(c.feature_type.value, DBFeatureType.UNKNOWN),
                    geometry=from_shape(Point(lon, lat), srid=4326),
                    outline=from_shape(outline_wgs84, srid=4326) if outline_wgs84 else None,
                    confidence=c.score,
                    depth_m=c.morphometrics.get("depth_m") or c.morphometrics.get("lrm_anomaly_m"),
                    area_m2=c.morphometrics.get("area_m2"),
                    circularity=c.morphometrics.get("circularity"),
                    wall_slope_deg=c.morphometrics.get("wall_slope_deg"),
                    source_passes=c.metadata.get("source_passes") if c.metadata else None,
                    morphometrics={k: float(v) if isinstance(v, (int, float)) else v
                                   for k, v in c.morphometrics.items()},
                )
                batch.append(det)
                if len(batch) >= 500:
                    session.add_all(batch)
                    await session.flush()
                    batch.clear()
            if batch:
                session.add_all(batch)
            await session.commit()
            return len(good)

    with profiler.stage("db_storage", parent="detection", detections=len(good)):
        stored = asyncio.run(_store())

    self.update_state(state="PROGRESS", meta={"percent": 100, "message": "Complete"})
    summary = profiler.log_summary()
    return {"raw_candidates": len(candidates), "stored_detections": stored, "profile": summary}


@app.task(bind=True, queue="detect")
def run_full_pipeline(self, job_id: str, region_name: str | None, pass_config: str, bbox_geojson: dict | None = None):
    """Full end-to-end: discover tiles → download → process → detect → store.

    This is what gets called when a user submits a job from the UI.
    Each invocation gets its own PipelineProfiler (process-local, no
    cross-worker sharing). The profiler summary is stored in the job's
    result_summary for inspection.
    """
    from hole_finder.db.models import Job, JobStatus
    from hole_finder.ingest.manager import get_source

    profiler = new_profiler(f"full_pipeline:{job_id[:8]}")

    def _update_job(status: str, progress: float, message: str = "", summary: dict | None = None, stage: str | None = None):
        async def _do():
            async with _async_session() as session:
                job = await session.get(Job, UUID(job_id))
                if job:
                    job.status = JobStatus(status.lower())
                    job.progress = progress
                    if status in ("COMPLETED", "FAILED"):
                        job.completed_at = datetime.now(UTC)
                    if summary:
                        job.result_summary = summary
                    elif stage:
                        # Merge stage into existing result_summary
                        existing = job.result_summary or {}
                        existing["stage"] = stage
                        job.result_summary = existing
                    if message and status == "FAILED":
                        job.error_message = message
                    await session.commit()
        asyncio.run(_do())

    try:
        _update_job("RUNNING", 5, "Discovering tiles", stage="discovering")

        # Discover tiles
        with profiler.stage("tile_discovery") as ctx:
            if region_name:
                from hole_finder.ingest.manager import discover_region
                tiles = asyncio.run(discover_region(region_name))
            elif bbox_geojson:
                from shapely.geometry import shape
                bbox = shape(bbox_geojson)
                source = get_source("usgs_3dep")
                tiles = []
                async def _discover():
                    async for t in source.discover_tiles(bbox):
                        tiles.append(t)
                asyncio.run(_discover())
            else:
                _update_job("FAILED", 0, "No region or bbox specified")
                return
            ctx["tiles_found"] = len(tiles)

        if not tiles:
            _update_job("COMPLETED", 100, summary={"tiles": 0, "detections": 0})
            return

        _update_job("RUNNING", 10, f"Downloading {len(tiles)} tiles", stage="downloading")

        # Download tiles — parallel, respect config tile_limit (consumer jobs: 4)
        async def _get_tile_limit():
            async with _async_session() as session:
                job = await session.get(Job, UUID(job_id))
                if job and job.config and "tile_limit" in job.config:
                    return job.config["tile_limit"]
            return 500

        config_limit = asyncio.run(_get_tile_limit())
        tile_limit = min(len(tiles), config_limit)
        downloaded = []
        with profiler.stage("tile_downloads", tile_limit=tile_limit) as ctx:
            source = get_source("usgs_3dep")
            dest = settings.raw_dir / "usgs_3dep"

            async def _download_all():
                import asyncio as aio
                sem = aio.Semaphore(16)  # 16 concurrent downloads (~678 Mbps available)
                results = []

                async def _dl(tile, idx):
                    async with sem:
                        t0 = time.perf_counter()
                        try:
                            path = await source.download_tile(tile, dest)
                            elapsed = time.perf_counter() - t0
                            log.info("tile_downloaded", tile=tile.filename,
                                     elapsed_s=round(elapsed, 2), index=idx+1)
                            return str(path)
                        except Exception as e:
                            log.warning("tile_download_failed",
                                        tile=tile.filename, error=str(e))
                            return None

                tasks = [_dl(tile, i) for i, tile in enumerate(tiles[:tile_limit])]
                results = await aio.gather(*tasks)
                return [r for r in results if r is not None]

            downloaded = asyncio.run(_download_all())
            _update_job("RUNNING", 40, f"Downloaded {len(downloaded)}/{tile_limit} tiles", stage="analyzing")
            ctx["downloaded"] = len(downloaded)
            ctx["failed"] = tile_limit - len(downloaded)

        if not downloaded:
            _update_job("FAILED", 40, "No tiles downloaded successfully")
            return

        # Process tiles — 8 in parallel via ThreadPoolExecutor.
        # Each tile's heavy work (PDAL, GDAL, WBT) runs as subprocesses
        # that release the GIL, so threads give true parallelism.
        # Can't use ProcessPoolExecutor (Celery daemonic process constraint).
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from hole_finder.processing.pipeline import ProcessingPipeline

        import hole_finder.detection.passes  # register passes

        from geoalchemy2.shape import from_shape
        from pyproj import Transformer
        from shapely.geometry import Point

        from hole_finder.db.models import Detection
        from hole_finder.db.models import FeatureType as DBFeatureType
        from hole_finder.detection.runner import PassRunner

        config_path = Path(f"/app/configs/passes/{pass_config}.toml")
        if not config_path.exists():
            config_path = settings.data_dir.parent / f"configs/passes/{pass_config}.toml"
        runner = PassRunner.from_toml(config_path)

        ft_map = {
            "sinkhole": DBFeatureType.SINKHOLE,
            "cave_entrance": DBFeatureType.CAVE_ENTRANCE,
            "mine_portal": DBFeatureType.MINE_PORTAL,
            "depression": DBFeatureType.DEPRESSION,
            "collapse_pit": DBFeatureType.COLLAPSE_PIT,
            "unknown": DBFeatureType.UNKNOWN,
        }

        # Thread-safe counter
        import threading
        _det_lock = threading.Lock()
        total_detections = 0
        completed_tiles = 0

        def _process_single_tile(i: int, tile_path: str) -> dict:
            """Process → detect → store → cleanup for one tile. Runs in a thread."""
            nonlocal total_detections, completed_tiles
            result = {"tile": tile_path, "index": i}

            # Process: PDAL → DEM → derivatives
            t0 = time.perf_counter()
            try:
                pipeline = ProcessingPipeline(output_dir=settings.processed_dir)
                input_path = Path(tile_path)
                if input_path.suffix in (".laz", ".las"):
                    tile_result = pipeline.process_point_cloud(input_path)
                else:
                    tile_result = pipeline.process_dem_file(input_path)
                result["process_s"] = round(time.perf_counter() - t0, 2)
                result["derivatives"] = len(tile_result.derivative_paths)
            except Exception as e:
                log.error("process_tile_failed", tile=tile_path, error=str(e))
                result["error"] = f"process: {e}"
                return result

            # Detect
            t0 = time.perf_counter()
            try:
                candidates = runner.run_on_dem(
                    tile_result.dem_path,
                    tile_result.derivative_paths,
                )

                crs_code = tile_result.crs or 32617
                transformer = Transformer.from_crs(f"EPSG:{crs_code}", "EPSG:4326", always_xy=True)
                good = [c for c in candidates
                        if c.score > 0.5
                        and c.morphometrics.get("area_m2", 0) > 100
                        and c.morphometrics.get("depth_m", 0) > 0.5
                        and (c.morphometrics.get("depth_m", 0)
                             or c.morphometrics.get("lrm_anomaly_m", 0)) < 100]
                # Keep only top 50 per tile, sorted by score
                good.sort(key=lambda c: c.score, reverse=True)
                good = good[:50]

                # Store detections
                async def _store():
                    async with _async_session() as session:
                        for c in good:
                            lon, lat = transformer.transform(c.geometry.x, c.geometry.y)
                            outline_wgs84 = _transform_outline(c.outline, transformer)
                            det = Detection(
                                feature_type=ft_map.get(c.feature_type.value, DBFeatureType.UNKNOWN),
                                geometry=from_shape(Point(lon, lat), srid=4326),
                                outline=from_shape(outline_wgs84, srid=4326) if outline_wgs84 else None,
                                confidence=c.score,
                                depth_m=c.morphometrics.get("depth_m") or c.morphometrics.get("lrm_anomaly_m"),
                                area_m2=c.morphometrics.get("area_m2"),
                                circularity=c.morphometrics.get("circularity"),
                                wall_slope_deg=c.morphometrics.get("wall_slope_deg"),
                                source_passes=c.metadata.get("source_passes") if c.metadata else None,
                                morphometrics={k: float(v) if isinstance(v, (int, float)) else v
                                               for k, v in c.morphometrics.items()},
                            )
                            session.add(det)
                        await session.commit()

                asyncio.run(_store())
                result["detect_s"] = round(time.perf_counter() - t0, 2)
                result["raw_candidates"] = len(candidates)
                result["stored"] = len(good)

                with _det_lock:
                    total_detections += len(good)
            except Exception as e:
                log.error("detect_tile_failed", tile=tile_path, error=str(e))
                result["error"] = f"detect: {e}"

            # Cleanup: delete raw + intermediate derivatives, keep DEM + hillshade
            freed_bytes = 0
            raw_path = Path(tile_path)
            if raw_path.exists():
                freed_bytes += raw_path.stat().st_size
                raw_path.unlink()

            keep_derivatives = {"hillshade"}
            if tile_result and tile_result.derivative_paths:
                for name, deriv_path in tile_result.derivative_paths.items():
                    if name in keep_derivatives:
                        continue
                    p = Path(deriv_path)
                    if p.exists():
                        freed_bytes += p.stat().st_size
                        p.unlink()

            if tile_result and tile_result.filled_dem_path:
                filled = Path(tile_result.filled_dem_path)
                if filled.exists():
                    freed_bytes += filled.stat().st_size
                    filled.unlink()

            result["freed_mb"] = round(freed_bytes / 1e6, 1)

            with _det_lock:
                completed_tiles += 1
            log.info("tile_complete", index=i, stored=result.get("stored", 0),
                     freed_mb=result["freed_mb"],
                     progress=f"{completed_tiles}/{len(downloaded)}")
            return result

        # Run 8 tiles in parallel
        PARALLEL_TILES = 8
        tile_results = []
        with profiler.stage("parallel_processing", parallel=PARALLEL_TILES) as pctx:
            with ThreadPoolExecutor(max_workers=PARALLEL_TILES) as executor:
                futures = {
                    executor.submit(_process_single_tile, i, tp): i
                    for i, tp in enumerate(downloaded)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        res = future.result()
                        tile_results.append(res)
                        # Update progress
                        pct = 40 + (len(tile_results) / len(downloaded)) * 55
                        tile_stage = "finishing" if pct > 90 else "analyzing"
                        _update_job("RUNNING", pct,
                                    f"Processed {len(tile_results)}/{len(downloaded)} tiles, "
                                    f"{total_detections} detections so far",
                                    stage=tile_stage)
                    except Exception as e:
                        log.error("tile_thread_failed", index=idx, error=str(e))
                        tile_results.append({"index": idx, "error": str(e)})

            pctx["tiles_ok"] = sum(1 for r in tile_results if "error" not in r)
            pctx["tiles_failed"] = sum(1 for r in tile_results if "error" in r)
            pctx["total_detections"] = total_detections

        profile_summary = profiler.log_summary()

        _update_job("COMPLETED", 100, summary={
            "tiles_discovered": len(tiles),
            "tiles_downloaded": len(downloaded),
            "total_detections": total_detections,
            "profile": profile_summary,
        })

    except Exception as e:
        _update_job("FAILED", 0, str(e)[:500])
        raise


@app.task(bind=True, queue="gpu")
def run_ml_pass(self, dem_path: str, pass_name: str, config: dict):
    """Run a single ML detection pass (GPU queue)."""
    from hole_finder.detection.base import PassInput
    from hole_finder.detection.registry import PassRegistry
    from hole_finder.utils.raster_io import read_dem

    self.update_state(state="PROGRESS", meta={"percent": 0, "message": f"Running {pass_name}"})

    dem, transform, crs = read_dem(Path(dem_path))

    pass_cls = PassRegistry.get(pass_name)
    detection_pass = pass_cls()

    pass_input = PassInput(
        dem=dem,
        transform=transform,
        crs=crs,
        derivatives={},
        config=config.get(f"passes.{pass_name}", {}),
    )

    candidates = detection_pass.run(pass_input)

    self.update_state(state="PROGRESS", meta={"percent": 100, "message": "Complete"})
    return {
        "pass_name": pass_name,
        "num_detections": len(candidates),
    }


@app.task(bind=True, queue="process")
def run_storage_eviction(self):
    """LRU storage eviction — runs daily via Celery Beat.

    Deletes tiles not accessed in 30 days, then caps total at 700GB
    by evicting oldest-accessed first.
    """
    from hole_finder.utils.storage import evict

    data_dir = settings.data_dir
    if not data_dir.exists():
        return {"skipped": True, "reason": "data_dir not found"}

    return evict(data_dir)
