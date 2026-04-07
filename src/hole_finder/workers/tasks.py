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
import math
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import numpy as np
import rasterio

from shapely.ops import transform as shapely_transform

from hole_finder.config import settings
from hole_finder.utils.crs import resolve_epsg
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
            crs_code = resolve_epsg(src.crs)

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
            and (c.morphometrics.get("depth_m", 0) or c.morphometrics.get("lrm_anomaly_m", 0)) < 100
            and c.morphometrics.get("circularity", 1.0) > 0.15
            and c.morphometrics.get("elongation", 1.0) > 0.2]

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
def run_full_pipeline(self, job_id: str, pass_config: str, bbox_geojson: dict):
    """Full end-to-end: discover tiles → download → process → detect → store.

    This is what gets called when a user submits a job from the UI.
    Source resolution: bbox center → FCC reverse geocode → state → sources.
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
            from shapely.geometry import shape
            from hole_finder.ingest.manager import discover_tiles_for_bbox
            bbox = shape(bbox_geojson)
            centroid = bbox.centroid
            # Get center from job config (more accurate than centroid for non-square bboxes)
            async def _get_center():
                async with _async_session() as session:
                    job = await session.get(Job, UUID(job_id))
                    if job and job.config:
                        return job.config.get("center_lat", centroid.y), job.config.get("center_lon", centroid.x)
                return centroid.y, centroid.x
            center_lat, center_lon = asyncio.run(_get_center())
            tiles, source_used = asyncio.run(discover_tiles_for_bbox(bbox, center_lat, center_lon))
            ctx["tiles_found"] = len(tiles)

        if not tiles:
            _update_job("FAILED", 0, "No LiDAR data found in this area. Try zooming out to a larger area or panning to a different location.", summary={"tiles": 0, "detections": 0})
            return

        # Compute tile limit early — needed for stale clearing and download
        async def _get_tile_limit():
            async with _async_session() as session:
                job = await session.get(Job, UUID(job_id))
                if job and job.config and "tile_limit" in job.config:
                    return job.config["tile_limit"]
            return 500
        config_limit = asyncio.run(_get_tile_limit())
        tile_limit = min(len(tiles), config_limit)

        # Sort tiles by distance to bbox center so we get radial coverage
        centroid = bbox.centroid
        tiles.sort(key=lambda t: t.bbox.centroid.distance(centroid))

        # Clear ALL stale data in scan area — detections + processed tiles on disk
        b = bbox.bounds
        async def _clear_stale():
            async with _async_session() as session:
                from sqlalchemy import text
                result = await session.execute(text("DELETE FROM detections WHERE ST_Within(geometry, ST_MakeEnvelope(:w, :s, :e, :n, 4326))"), {"w": b[0], "s": b[1], "e": b[2], "n": b[3]})
                deleted = result.rowcount
                await session.commit()
                if deleted:
                    log.info("stale_detections_cleared", count=deleted, bbox=[round(v, 4) for v in b])
        asyncio.run(_clear_stale())
        # Wipe processed tile dirs that will be re-downloaded (forces fresh processing)
        import shutil
        for tile in tiles[:tile_limit]:
            stem = tile.filename.replace(".copc.laz", "").replace(".laz", "").replace(".las", "")
            tile_dir = settings.processed_dir / stem
            if tile_dir.exists():
                shutil.rmtree(tile_dir, ignore_errors=True)
                log.info("stale_tile_dir_cleared", dir=str(tile_dir))

        source_name = source_used
        _update_job("RUNNING", 10, f"Downloading {len(tiles)} tiles", stage="downloading",
                     summary={"stage": "downloading", "source": source_name})
        downloaded = []
        with profiler.stage("tile_downloads", tile_limit=tile_limit) as ctx:
            dl_source_name = source_used
            source = get_source(dl_source_name)
            dest = settings.raw_dir / dl_source_name

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
                            size_bytes = Path(path).stat().st_size if path else 0
                            log.info("tile_downloaded", tile=tile.filename,
                                     elapsed_s=round(elapsed, 2), index=idx+1,
                                     size_mb=round(size_bytes / 1e6, 1))
                            return (str(path), size_bytes)
                        except Exception as e:
                            log.warning("tile_download_failed",
                                        tile=tile.filename, error=str(e))
                            return None

                tasks = [_dl(tile, i) for i, tile in enumerate(tiles[:tile_limit])]
                results = await aio.gather(*tasks)
                return [(p, s) for p, s in [r for r in results if r is not None]]

            dl_results = asyncio.run(_download_all())
            downloaded = [p for p, _ in dl_results]
            total_download_bytes = sum(s for _, s in dl_results)
            dl_mb = round(total_download_bytes / 1e6, 1)
            _update_job("RUNNING", 40, f"Downloaded {len(downloaded)}/{tile_limit} tiles ({dl_mb} MB)", stage="analyzing",
                         summary={"stage": "analyzing", "source": source_name, "download_mb": dl_mb})
            ctx["downloaded"] = len(downloaded)
            ctx["failed"] = tile_limit - len(downloaded)
            ctx["download_mb"] = dl_mb

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
            tile_result = None
            try:
                log.info("processing_tile", index=i, tile=Path(tile_path).name, size_mb=round(Path(tile_path).stat().st_size / 1e6, 1))
                pipeline = ProcessingPipeline(output_dir=settings.processed_dir)
                input_path = Path(tile_path)
                if input_path.suffix in (".laz", ".las"):
                    tile_result = pipeline.process_point_cloud(input_path)
                else:
                    tile_result = pipeline.process_dem_file(input_path)
                result["process_s"] = round(time.perf_counter() - t0, 2)
                result["derivatives"] = len(tile_result.derivative_paths)
                # CRS details logging — track how CRS was resolved for each tile
                _crs_is_compound = False
                try:
                    import rasterio as _rio_check
                    with _rio_check.open(tile_result.dem_path) as _src_check:
                        _raw_crs = _src_check.crs
                        _raw_epsg = _raw_crs.to_epsg() if _raw_crs else None
                        from pyproj import CRS as _PyprojCRS
                        _pcrs = _PyprojCRS(_raw_crs) if _raw_crs else None
                        _crs_is_compound = _pcrs.is_compound if _pcrs else False
                    log.info("tile_crs_details", tile=Path(tile_path).name, resolved_epsg=tile_result.crs, raw_epsg=_raw_epsg, is_compound=_crs_is_compound, raw_crs=str(_raw_crs)[:100] if _raw_crs else "None")
                except Exception:
                    log.info("tile_crs_details", tile=Path(tile_path).name, resolved_epsg=tile_result.crs, raw_epsg="unknown", is_compound=False)
                log.info("processing_complete", index=i, tile=Path(tile_path).name, process_s=result["process_s"], derivatives=list(tile_result.derivative_paths.keys()), crs=tile_result.crs, dem=str(tile_result.dem_path))
            except Exception as e:
                log.error("process_tile_failed", tile=tile_path, error=str(e))
                result["error"] = f"process: {e}"
                result["error_type"] = "process"
                return result

            # Detect — with per-phase timing
            t0 = time.perf_counter()
            _timings = {}
            try:
                log.info("detection_starting", tile=Path(tile_path).stem, dem=str(tile_result.dem_path), crs=tile_result.crs, derivatives=list(tile_result.derivative_paths.keys()))
                _t = time.perf_counter()
                candidates = runner.run_on_dem(
                    tile_result.dem_path,
                    tile_result.derivative_paths,
                )
                _timings["detection_passes_s"] = round(time.perf_counter() - _t, 3)
                log.info("detection_raw", tile=Path(tile_path).stem, raw_candidates=len(candidates))

                _t = time.perf_counter()
                crs_code = tile_result.crs
                transformer = Transformer.from_crs(f"EPSG:{crs_code}", "EPSG:4326", always_xy=True)
                import rasterio as _rio
                with _rio.open(tile_result.dem_path) as _src:
                    _bnd = _src.bounds
                _test_lon, _test_lat = transformer.transform(_bnd.left, _bnd.bottom)
                if not (math.isfinite(_test_lon) and math.isfinite(_test_lat)):
                    raise RuntimeError(f"CRS transform produces infinity: EPSG:{crs_code} ({_bnd.left},{_bnd.bottom}) -> ({_test_lon},{_test_lat})")
                log.info("crs_transform_ok", crs=crs_code, test_point=f"({round(_test_lon,4)},{round(_test_lat,4)})")

                good = [c for c in candidates
                        if c.score > 0.15
                        and c.morphometrics.get("area_m2", 0) > 20
                        and c.morphometrics.get("depth_m", 0) > 0.3
                        and (c.morphometrics.get("depth_m", 0)
                             or c.morphometrics.get("lrm_anomaly_m", 0)) < 150]
                log.info("detection_filtered", tile=Path(tile_path).stem, after_filter=len(good), before_filter=len(candidates))
                good.sort(key=lambda c: c.score, reverse=True)
                good = good[:200]

                # Transform centroids to WGS84, discard any with infinity/NaN coords
                wgs84_points = []
                for c in good:
                    lon, lat = transformer.transform(c.geometry.x, c.geometry.y)
                    if not (math.isfinite(lon) and math.isfinite(lat)):
                        log.warning("infinite_coord_skipped", geom_x=c.geometry.x, geom_y=c.geometry.y, score=round(c.score, 2))
                        wgs84_points.append(None)
                    else:
                        wgs84_points.append((lon, lat))
                paired = [(c, p) for c, p in zip(good, wgs84_points) if p is not None]
                good = [c for c, _ in paired]
                wgs84_points = [p for _, p in paired]
                _timings["crs_transform_s"] = round(time.perf_counter() - _t, 3)

                # Filter out detections on buildings using OSM data
                _t = time.perf_counter()
                if wgs84_points:
                    from hole_finder.detection.postprocess.building_filter import fetch_building_polygons
                    from shapely.prepared import prep as shapely_prep
                    from shapely.geometry import MultiPolygon
                    lons = [p[0] for p in wgs84_points]
                    lats = [p[1] for p in wgs84_points]
                    buildings = fetch_building_polygons(min(lons), min(lats), max(lons), max(lats))
                    if buildings:
                        merged = shapely_prep(MultiPolygon(buildings))
                        keep = []
                        for c, (lon, lat) in zip(good, wgs84_points):
                            if not merged.contains(Point(lon, lat)):
                                keep.append((c, lon, lat))
                            else:
                                log.info("building_filtered", lon=round(lon, 5), lat=round(lat, 5), score=round(c.score, 2))
                        log.info("building_filter_result", before=len(good), after=len(keep), removed=len(good) - len(keep))
                        good_with_coords = keep
                    else:
                        good_with_coords = list(zip(good, [p[0] for p in wgs84_points], [p[1] for p in wgs84_points]))
                else:
                    good_with_coords = []
                _timings["building_filter_s"] = round(time.perf_counter() - _t, 3)

                # Filter out detections on roads, waterways, and railways
                _t = time.perf_counter()
                if good_with_coords:
                    from hole_finder.detection.postprocess.infrastructure_filter import filter_candidates_by_infrastructure
                    candidates_for_infra = [item[0] for item in good_with_coords]
                    coords_for_infra = [(item[1], item[2]) for item in good_with_coords]
                    lons_i = [c[0] for c in coords_for_infra]
                    lats_i = [c[1] for c in coords_for_infra]
                    good_with_coords = filter_candidates_by_infrastructure(candidates_for_infra, coords_for_infra, min(lons_i), min(lats_i), max(lons_i), max(lats_i))
                _timings["infra_filter_s"] = round(time.perf_counter() - _t, 3)

                # Store detections
                _t = time.perf_counter()
                log.info("storing_detections", tile=Path(tile_path).stem, count=len(good_with_coords))
                async def _store():
                    stored = 0
                    async with _async_session() as session:
                        for item in good_with_coords:
                            if len(item) == 3:
                                c, lon, lat = item
                            else:
                                c = item[0]
                                lon, lat = transformer.transform(c.geometry.x, c.geometry.y)
                            if not (math.isfinite(lon) and math.isfinite(lat)):
                                log.warning("infinite_coord_in_store", lon=lon, lat=lat)
                                continue
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
                            stored += 1
                        await session.commit()
                    return stored
                stored_count = asyncio.run(_store())
                _timings["db_store_s"] = round(time.perf_counter() - _t, 3)
                _timings["total_detect_s"] = round(time.perf_counter() - t0, 3)
                result["detect_s"] = _timings["total_detect_s"]
                result["raw_candidates"] = len(candidates)
                result["stored"] = stored_count
                log.info("tile_detections_stored", tile=Path(tile_path).stem, stored=stored_count, raw=len(candidates), filtered=len(good))
                log.info("tile_phase_timing", tile=Path(tile_path).name, process_s=result.get("process_s"), **_timings)

                with _det_lock:
                    total_detections += stored_count
                # Per-tile quality report
                log.info("tile_quality_report", tile=Path(tile_path).name, derivatives_ok=len(tile_result.derivative_paths), raw_candidates=len(candidates), filtered=len(good), stored=stored_count, crs=crs_code, overpass_status="ok" if good_with_coords else "skipped_or_empty")
            except Exception as e:
                _err_str = str(e)
                _err_type = "crs_infinity" if "infinity" in _err_str else "detect_other"
                log.error("detect_tile_failed", tile=tile_path, error=_err_str, error_type=_err_type)
                result["error"] = f"detect: {e}"
                result["error_type"] = _err_type

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

        # Each PDAL subprocess uses ~6 GB RAM. 6 × 6 GB = ~36 GB peak on 64 GB box.
        PARALLEL_TILES = 6
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

        # Categorize errors for the pipeline summary
        tile_errors = [r["error"] for r in tile_results if "error" in r][:5]
        error_types = {}
        for r in tile_results:
            if "error_type" in r:
                etype = r["error_type"]
                error_types[etype] = error_types.get(etype, 0) + 1
        tiles_ok = sum(1 for r in tile_results if "error" not in r)
        tiles_failed = sum(1 for r in tile_results if "error" in r)
        log.info("pipeline_error_summary", job_id=job_id[:8], tiles_ok=tiles_ok, tiles_failed=tiles_failed, total_detections=total_detections, error_types=error_types if error_types else None)
        _update_job("COMPLETED", 100, summary={
            "tiles_discovered": len(tiles),
            "tiles_downloaded": len(downloaded),
            "download_mb": dl_mb,
            "total_detections": total_detections,
            "tiles_ok": tiles_ok,
            "tiles_failed": tiles_failed,
            "error_types": error_types if error_types else None,
            "tile_errors": tile_errors if tile_errors else None,
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
