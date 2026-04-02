"""Processing pipeline — orchestrates PDAL + GDAL + WhiteboxTools.

Python only orchestrates subprocesses and reads results.
All derivative computation is in derivatives.py.
"""

import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from hole_finder.config import settings
from hole_finder.processing.derivatives import compute_all_derivatives, fill_depressions
from hole_finder.utils.logging import log
from hole_finder.utils.perf import PipelineProfiler, get_profiler, new_profiler


@dataclass
class ProcessedTile:
    """Immutable result of processing a tile. Stored permanently."""

    tile_dir: Path
    dem_path: Path
    filled_dem_path: Path | None = None
    derivative_paths: dict[str, Path] = field(default_factory=dict)
    resolution_m: float = 1.0
    crs: int = 32617


def generate_dem_pdal(
    input_path: Path,
    output_dir: Path,
    resolution: float = 1.0,
    target_srs: str | None = None,
) -> tuple[Path, Path]:
    """Generate ground DEM and filled DEM from point cloud using PDAL."""
    profiler = get_profiler()
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = input_path.stem.replace(".copc", "")
    dem_path = output_dir / f"{stem}_dem.tif"
    filled_path = output_dir / f"{stem}_filled.tif"

    if not dem_path.exists():
        pipeline = [
            {"type": "readers.copc" if str(input_path).endswith(".copc.laz") else "readers.las",
             "filename": str(input_path)},
        ]
        if target_srs:
            pipeline.append({"type": "filters.reprojection", "out_srs": target_srs})
        pipeline.extend([
            {"type": "filters.smrf", "slope": 0.15, "window": 18, "threshold": 0.5},
            {"type": "filters.range", "limits": "Classification[2:2]"},
            {"type": "writers.gdal", "filename": str(dem_path), "resolution": resolution,
             "output_type": "idw", "gdalopts": "COMPRESS=DEFLATE,TILED=YES,BLOCKXSIZE=256,BLOCKYSIZE=256",
             "data_type": "float32"},
        ])
        log.info("pdal_dem_start", input=str(input_path), file_size_mb=round(input_path.stat().st_size / 1e6, 1))
        t0 = time.perf_counter()
        proc = subprocess.run(
            ["pdal", "pipeline", "--stdin"],
            input=json.dumps({"pipeline": pipeline}),
            capture_output=True, text=True, timeout=900,
        )
        pdal_elapsed = time.perf_counter() - t0
        if proc.returncode != 0:
            raise RuntimeError(f"PDAL DEM failed: {proc.stderr[:500]}")
        log.info("pdal_dem_complete", elapsed_s=round(pdal_elapsed, 2), output=str(dem_path))
        if profiler:
            profiler.record("pdal_dem_generation", pdal_elapsed, parent="processing")
    else:
        log.info("pdal_dem_cached", path=str(dem_path))

    if not filled_path.exists():
        t0 = time.perf_counter()
        _result, fill_elapsed = fill_depressions(str(dem_path), str(filled_path))
        log.info("fill_depressions_complete", elapsed_s=round(fill_elapsed, 2))
        if profiler:
            profiler.record("fill_depressions", fill_elapsed, parent="processing")
    else:
        log.info("fill_depressions_cached", path=str(filled_path))

    return dem_path, filled_path


class ProcessingPipeline:
    """Full tile processing — PDAL + GDAL + WhiteboxTools.

    All outputs are written to persistent storage. Once computed,
    derivatives are cached permanently and never recomputed unless
    explicitly requested via force=True.
    """

    def __init__(
        self,
        output_dir: Path,
        resolution: float = 1.0,
        target_srs: str | None = None,
    ):
        self.output_dir = output_dir
        self.resolution = resolution
        self.target_srs = target_srs

    def process_point_cloud(self, input_path: Path, force: bool = False) -> ProcessedTile:
        """Process a LAZ/COPC point cloud through the full pipeline."""
        stem = input_path.stem.replace(".copc", "")
        tile_dir = self.output_dir / stem
        tile_dir.mkdir(parents=True, exist_ok=True)
        deriv_dir = tile_dir / "derivatives"

        marker = tile_dir / ".processed"
        if marker.exists() and not force and settings.enable_processing_cache:
            cached = self._load_existing(tile_dir, deriv_dir)
            if cached.dem_path.exists() and len(cached.derivative_paths) >= 8:
                return cached
            log.warning("stale_cache_reprocessing", tile_dir=str(tile_dir), derivatives=len(cached.derivative_paths))
            marker.unlink(missing_ok=True)

        profiler = new_profiler(f"process_point_cloud:{stem}")

        with profiler.stage("dem_generation", parent="processing", input=str(input_path)):
            dem_path, filled_path = generate_dem_pdal(
                input_path, tile_dir, self.resolution, self.target_srs
            )

        with profiler.stage("derivatives_all", parent="processing"):
            derivative_paths = compute_all_derivatives(dem_path, filled_path, deriv_dir)

        marker.write_text(f"processed\nderivatives: {len(derivative_paths)}\n")
        profiler.log_summary()

        return ProcessedTile(
            tile_dir=tile_dir, dem_path=dem_path, filled_dem_path=filled_path,
            derivative_paths=derivative_paths, resolution_m=self.resolution,
        )

    def process_dem_file(self, dem_path: Path, force: bool = False) -> ProcessedTile:
        """Process from an existing DEM file (no PDAL needed)."""
        stem = dem_path.stem
        tile_dir = self.output_dir / stem
        tile_dir.mkdir(parents=True, exist_ok=True)
        deriv_dir = tile_dir / "derivatives"

        marker = tile_dir / ".processed"
        if marker.exists() and not force and settings.enable_processing_cache:
            cached = self._load_existing(tile_dir, deriv_dir)
            if cached.dem_path.exists() and len(cached.derivative_paths) >= 8:
                return cached
            log.warning("stale_cache_reprocessing", tile_dir=str(tile_dir), derivatives=len(cached.derivative_paths))
            marker.unlink(missing_ok=True)

        profiler = new_profiler(f"process_dem:{stem}")

        filled_path = tile_dir / f"{stem}_filled.tif"
        if not filled_path.exists():
            with profiler.stage("fill_depressions", parent="processing"):
                _result, elapsed = fill_depressions(str(dem_path), str(filled_path))

        with profiler.stage("derivatives_all", parent="processing"):
            derivative_paths = compute_all_derivatives(dem_path, filled_path, deriv_dir)

        marker.write_text(f"processed\nderivatives: {len(derivative_paths)}\n")
        profiler.log_summary()

        return ProcessedTile(
            tile_dir=tile_dir, dem_path=dem_path, filled_dem_path=filled_path,
            derivative_paths=derivative_paths, resolution_m=self.resolution,
        )

    def _load_existing(self, tile_dir: Path, deriv_dir: Path) -> ProcessedTile:
        """Load an already-processed tile from disk."""
        dem_files = list(tile_dir.glob("*_dem.tif")) + list(tile_dir.glob("dem_*.tif"))
        dem_path = dem_files[0] if dem_files else tile_dir / "dem.tif"

        filled_files = list(tile_dir.glob("*_filled.tif"))
        filled_path = filled_files[0] if filled_files else None

        derivative_paths = {}
        if deriv_dir.exists():
            for f in deriv_dir.glob("*.tif"):
                derivative_paths[f.stem] = f

        log.info("pipeline_cached", tile_dir=str(tile_dir), derivatives=len(derivative_paths))
        return ProcessedTile(
            tile_dir=tile_dir, dem_path=dem_path, filled_dem_path=filled_path,
            derivative_paths=derivative_paths, resolution_m=self.resolution,
        )
