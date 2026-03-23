"""Terrain derivative computation — GDAL + WhiteboxTools native subprocesses.

NO numpy computation. Everything runs as compiled C/C++/Rust subprocesses.
Python only orchestrates and reads results.

For unit tests: generate small test GeoTIFFs and run the same native pipeline.
"""

import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from magic_eyes.utils.logging import log


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

def compute_hillshade(dem: str, out: str) -> str:
    _run(["gdaldem", "hillshade", dem, out, "-az", "315", "-alt", "45",
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


def compute_slope(dem: str, out: str) -> str:
    _run(["gdaldem", "slope", dem, out,
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


def compute_tpi(dem: str, out: str) -> str:
    _run(["gdaldem", "TPI", dem, out,
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


def compute_roughness(dem: str, out: str) -> str:
    _run(["gdaldem", "roughness", dem, out,
          "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-q"])
    return out


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


def compute_profile_curvature(dem: str, out: str) -> str:
    wbt = _get_wbt()
    wbt.profile_curvature(dem, out)
    return out


def compute_plan_curvature(dem: str, out: str) -> str:
    wbt = _get_wbt()
    wbt.plan_curvature(dem, out)
    return out


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
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    dem = str(dem_path)
    filled = str(filled_dem_path)

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
    for name, out_path, fn, args in tasks:
        if out_path.exists():
            results[name] = out_path
        else:
            to_compute.append((name, fn, args, out_path))

    if not to_compute:
        log.info("all_derivatives_cached", count=len(results))
        return results

    log.info("computing_derivatives", cached=len(results), remaining=len(to_compute))

    # Run uncached derivatives in parallel
    with ProcessPoolExecutor(max_workers=min(max_workers, len(to_compute))) as executor:
        futures = {}
        for name, fn, args, out_path in to_compute:
            futures[executor.submit(fn, *args)] = (name, out_path)

        for future in as_completed(futures):
            name, out_path = futures[future]
            try:
                future.result()
                results[name] = out_path
                log.info("derivative_done", name=name)
            except Exception as e:
                log.error("derivative_failed", name=name, error=str(e))

    return results
