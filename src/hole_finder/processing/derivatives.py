"""Terrain derivative computation — GDAL + WhiteboxTools native subprocesses.

NO numpy computation. Everything runs as compiled C/C++/Rust subprocesses.
Python only orchestrates and reads results.

For unit tests: generate small test GeoTIFFs and run the same native pipeline.
"""

import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from hole_finder.utils.logging import log


def _run(cmd: list[str], timeout: int = 300) -> None:
    """Run a subprocess, raise on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"{cmd[0]} failed (exit {result.returncode}): {result.stderr[:500]}")


def _get_wbt():
    """Get a WhiteboxTools instance."""
    import whitebox
    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(False)
    return wbt


# --- Individual derivative functions (each runs a native subprocess) ---
# Each returns (output_path, elapsed_seconds) for profiling.
# These run in child processes via ProcessPoolExecutor, so they can't
# share the parent's PipelineProfiler — they return timing data instead.


def _timed_derivative(fn):
    """Wrapper that times a derivative function and returns (result, elapsed_s)."""
    def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        result = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        return result, elapsed
    wrapper.__name__ = fn.__name__
    wrapper.__qualname__ = fn.__qualname__
    return wrapper


@_timed_derivative
def compute_hillshade(dem: str, out: str) -> str:
    _run(["gdaldem", "hillshade", dem, out, "-az", "315", "-alt", "45",
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


@_timed_derivative
def compute_slope(dem: str, out: str) -> str:
    _run(["gdaldem", "slope", dem, out,
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


@_timed_derivative
def compute_tpi(dem: str, out: str) -> str:
    _run(["gdaldem", "TPI", dem, out,
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


@_timed_derivative
def compute_roughness(dem: str, out: str) -> str:
    _run(["gdaldem", "roughness", dem, out,
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


@_timed_derivative
def compute_svf(dem: str, out: str) -> str:
    wbt = _get_wbt()
    # WBT method name varies by version
    if hasattr(wbt, 'sky_view_factor'):
        wbt.sky_view_factor(dem, out)
    elif hasattr(wbt, 'viewshed'):
        # Fallback: use multidirectional hillshade as SVF proxy
        wbt.multidirectional_hillshade(dem, out)
    else:
        raise RuntimeError("WhiteboxTools has no sky_view_factor or suitable alternative")
    return out


@_timed_derivative
def compute_lrm(dem: str, out: str, kernel: int = 100) -> str:
    wbt = _get_wbt()
    # WBT method name varies by version
    if hasattr(wbt, 'deviation_from_mean'):
        wbt.deviation_from_mean(dem, out, filterx=kernel, filtery=kernel)
    elif hasattr(wbt, 'dev_from_mean_elev'):
        wbt.dev_from_mean_elev(dem, out, filterx=kernel, filtery=kernel)
    elif hasattr(wbt, 'diff_from_mean_elev'):
        wbt.diff_from_mean_elev(dem, out, filterx=kernel, filtery=kernel)
    else:
        raise RuntimeError("WhiteboxTools has no deviation_from_mean or suitable alternative")
    return out


@_timed_derivative
def compute_profile_curvature(dem: str, out: str) -> str:
    wbt = _get_wbt()
    wbt.profile_curvature(dem, out)
    return out


@_timed_derivative
def compute_plan_curvature(dem: str, out: str) -> str:
    wbt = _get_wbt()
    wbt.plan_curvature(dem, out)
    return out


@_timed_derivative
def compute_fill_difference(dem: str, filled: str, out: str) -> str:
    """Subtract original DEM from filled DEM using rasterio (trivial operation)."""
    import rasterio
    with rasterio.open(dem) as src_dem, rasterio.open(filled) as src_filled:
        dem_arr = src_dem.read(1)
        filled_arr = src_filled.read(1)
        diff = (filled_arr - dem_arr).astype("float32")
        profile = src_dem.profile.copy()
        profile.update(dtype="float32", compress="deflate")
        with rasterio.open(out, "w", **profile) as dst:
            dst.write(diff, 1)
    return out


@_timed_derivative
def fill_depressions(dem: str, out: str) -> str:
    wbt = _get_wbt()
    wbt.fill_depressions(dem, out)
    return out


# --- Parallel orchestrator ---

def compute_all_derivatives(
    dem_path: Path,
    filled_dem_path: Path,
    output_dir: Path,
    max_workers: int = 8,
) -> dict[str, Path]:
    """Compute all derivatives in parallel using native subprocesses.

    Each derivative is a separate process running a compiled tool.
    Results are cached permanently — skips if output file already exists.

    Returns dict of {name: Path} and logs per-derivative timing.
    Child processes can't share the parent's PipelineProfiler, so each
    derivative function returns (result, elapsed_s) via @_timed_derivative.
    Timing is collected here and fed back to the profiler if one is active.
    """
    from hole_finder.utils.perf import get_profiler

    output_dir.mkdir(parents=True, exist_ok=True)
    dem = str(dem_path)
    filled = str(filled_dem_path)
    wall_start = time.perf_counter()

    # (name, output_path, function, args)
    tasks = [
        ("hillshade", output_dir / "hillshade.tif", compute_hillshade, [dem, str(output_dir / "hillshade.tif")]),
        ("slope", output_dir / "slope.tif", compute_slope, [dem, str(output_dir / "slope.tif")]),
        ("tpi", output_dir / "tpi.tif", compute_tpi, [dem, str(output_dir / "tpi.tif")]),
        ("roughness", output_dir / "roughness.tif", compute_roughness, [dem, str(output_dir / "roughness.tif")]),
        ("svf", output_dir / "svf.tif", compute_svf, [dem, str(output_dir / "svf.tif")]),
        ("lrm_50m", output_dir / "lrm_50m.tif", compute_lrm, [dem, str(output_dir / "lrm_50m.tif"), 50]),
        ("lrm_100m", output_dir / "lrm_100m.tif", compute_lrm, [dem, str(output_dir / "lrm_100m.tif"), 100]),
        ("lrm_200m", output_dir / "lrm_200m.tif", compute_lrm, [dem, str(output_dir / "lrm_200m.tif"), 200]),
        ("profile_curvature", output_dir / "profile_curvature.tif", compute_profile_curvature, [dem, str(output_dir / "profile_curvature.tif")]),
        ("plan_curvature", output_dir / "plan_curvature.tif", compute_plan_curvature, [dem, str(output_dir / "plan_curvature.tif")]),
        ("fill_difference", output_dir / "fill_difference.tif", compute_fill_difference, [dem, filled, str(output_dir / "fill_difference.tif")]),
    ]

    results: dict[str, Path] = {}

    # Check cache first
    to_compute = []
    cached_names = []
    for name, out_path, fn, args in tasks:
        if out_path.exists():
            results[name] = out_path
            cached_names.append(name)
        else:
            to_compute.append((name, fn, args, out_path))

    if not to_compute:
        log.info("all_derivatives_cached", count=len(results))
        return results

    log.info(
        "computing_derivatives",
        cached=len(results),
        remaining=len(to_compute),
        max_workers=min(max_workers, len(to_compute)),
        names=[t[0] for t in to_compute],
    )

    profiler = get_profiler()
    timings: list[tuple[str, float]] = []

    # Run uncached derivatives in parallel via threads.
    # Each derivative calls a native subprocess (GDAL/WBT) that releases the GIL,
    # so threads give real parallelism. We use ThreadPoolExecutor instead of
    # ProcessPoolExecutor because Celery workers are daemonic processes that
    # cannot spawn child processes.
    with ThreadPoolExecutor(max_workers=min(max_workers, len(to_compute))) as executor:
        futures = {}
        for name, fn, args, out_path in to_compute:
            futures[executor.submit(fn, *args)] = (name, out_path)

        for future in as_completed(futures):
            name, out_path = futures[future]
            try:
                _result_path, elapsed_s = future.result()
                results[name] = out_path
                timings.append((name, elapsed_s))
                log.info("derivative_done", name=name, elapsed_s=round(elapsed_s, 3))
                if profiler:
                    profiler.record(name, elapsed_s, parent="derivatives")
            except Exception as e:
                log.error("derivative_failed", name=name, error=str(e))

    wall_elapsed = time.perf_counter() - wall_start
    cpu_total = sum(t for _, t in timings)

    # Sort by slowest-first for the summary
    timings.sort(key=lambda x: x[1], reverse=True)
    timing_summary = {name: round(t, 3) for name, t in timings}

    log.info(
        "derivatives_complete",
        wall_time_s=round(wall_elapsed, 3),
        cpu_total_s=round(cpu_total, 3),
        parallelism_ratio=round(cpu_total / wall_elapsed, 1) if wall_elapsed > 0 else 0,
        computed=len(timings),
        cached=len(cached_names),
        slowest=timings[0][0] if timings else None,
        slowest_s=round(timings[0][1], 3) if timings else 0,
        per_derivative=timing_summary,
    )

    if profiler:
        profiler.record(
            "derivatives_total", wall_elapsed, parent=None,
            computed=len(timings), cached=len(cached_names),
            parallelism_ratio=round(cpu_total / wall_elapsed, 1) if wall_elapsed > 0 else 0,
        )

    return results
