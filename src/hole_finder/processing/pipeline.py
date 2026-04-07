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
from hole_finder.utils.crs import resolve_epsg
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


def _ensure_geotiff_keys(dem_path: Path) -> None:
    """Re-encode DEM with proper GeoTIFF keys if it only has WKT CRS.
    WhiteboxTools panics on 'TIFF file does not contain geokeys' when PDAL
    writes compound CRS (UTM + NAVD88) as WKT-only. Extract horizontal EPSG
    and re-write via gdal_translate to embed GeoKeys."""
    import re
    import rasterio
    with rasterio.open(dem_path) as src:
        crs = src.crs
        if not crs:
            return
        epsg = crs.to_epsg()
        if epsg:
            return  # Already has a clean EPSG → GeoKeys are fine
    # Extract horizontal EPSG from compound CRS
    from pyproj import CRS as PyprojCRS
    pcrs = PyprojCRS(crs)
    horiz = pcrs.sub_crs_list[0] if pcrs.is_compound and pcrs.sub_crs_list else pcrs
    h_epsg = horiz.to_epsg()
    if not h_epsg:
        m = re.search(r'UTM zone (\d+)([NS])', str(crs))
        if m:
            h_epsg = 26900 + int(m.group(1)) if m.group(2) == 'N' else 32700 + int(m.group(1))
    if not h_epsg:
        log.warning("geotiff_keys_unknown_crs", path=str(dem_path))
        return
    tmp = dem_path.with_suffix('.tmp.tif')
    result = subprocess.run(["gdal_translate", "-a_srs", f"EPSG:{h_epsg}", "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", str(dem_path), str(tmp)], capture_output=True, text=True, timeout=120)
    if result.returncode == 0 and tmp.exists():
        tmp.rename(dem_path)
        log.info("geotiff_keys_fixed", path=str(dem_path), epsg=h_epsg)
    else:
        tmp.unlink(missing_ok=True)
        log.warning("geotiff_keys_fix_failed", path=str(dem_path), error=result.stderr[:200])


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
        # Fix zeroed return values (LAS 1.4 spec requires >=1) — some sources
        # (e.g. NC NOAA 2015 Phase 3) have points with NumberOfReturns=0 which
        # causes SMRF to reject the entire tile. Assign single-return values.
        pipeline.append({"type": "filters.assign", "value": ["ReturnNumber = 1 WHERE ReturnNumber == 0", "NumberOfReturns = 1 WHERE NumberOfReturns == 0"]})
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
        # Ensure DEM has GeoTIFF keys (not just WKT) — WhiteboxTools panics without them.
        # Compound CRS from PDAL (UTM + NAVD88) writes WKT only. Re-encode with horizontal EPSG.
        _ensure_geotiff_keys(dem_path)
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
            crs=self._read_crs(dem_path),
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
            crs=self._read_crs(dem_path),
        )

    @staticmethod
    def _read_crs(dem_path: Path) -> int:
        """Read EPSG code from a DEM file via robust compound CRS resolver."""
        try:
            return resolve_epsg(dem_path)
        except ValueError as e:
            log.error("pipeline_crs_unresolvable", path=str(dem_path), error=str(e))
            raise

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
            crs=self._read_crs(dem_path) if dem_path.exists() else 32617,
        )
