"""Terrain derivative computation from DEMs — all pure numpy/scipy.

Each function takes a DEM array and resolution, returns a derived raster.
These are the inputs to detection passes.

References:
- Slope/aspect: Horn (1981) method
- Curvature: Zevenbergen & Thorne (1987)
- SVF: Zakšek et al. (2011), SAGA GIS algorithm
- TPI: Weiss (2001)
- LRM: Moyes & Montgomery (2019)
"""

import numpy as np
from numpy.typing import NDArray
from scipy.ndimage import uniform_filter


def compute_hillshade(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    azimuth: float = 315.0,
    altitude: float = 45.0,
) -> NDArray[np.float32]:
    """Compute analytical hillshade.

    Args:
        dem: elevation array
        resolution: cell size in meters
        azimuth: sun azimuth in degrees (0=N, clockwise)
        altitude: sun altitude angle in degrees above horizon
    """
    az_rad = np.radians(360 - azimuth + 90)  # convert to math angle
    alt_rad = np.radians(altitude)

    # Gradient using central differences
    dy, dx = np.gradient(dem, resolution)

    slope = np.arctan(np.sqrt(dx**2 + dy**2))
    aspect = np.arctan2(-dy, dx)

    hillshade = (
        np.sin(alt_rad) * np.cos(slope)
        + np.cos(alt_rad) * np.sin(slope) * np.cos(az_rad - aspect)
    )

    return np.clip(hillshade * 255, 0, 255).astype(np.float32)


def compute_slope(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    degrees: bool = True,
) -> NDArray[np.float32]:
    """Compute slope using Horn's method (3x3 finite difference).

    Returns slope in degrees (default) or radians.
    """
    # Pad to handle edges
    padded = np.pad(dem, 1, mode="edge")

    # Horn's 3x3 weighted differences
    z1 = padded[:-2, :-2]
    z2 = padded[:-2, 1:-1]
    z3 = padded[:-2, 2:]
    z4 = padded[1:-1, :-2]
    z6 = padded[1:-1, 2:]
    z7 = padded[2:, :-2]
    z8 = padded[2:, 1:-1]
    z9 = padded[2:, 2:]

    dz_dx = ((z3 + 2 * z6 + z9) - (z1 + 2 * z4 + z7)) / (8 * resolution)
    dz_dy = ((z7 + 2 * z8 + z9) - (z1 + 2 * z2 + z3)) / (8 * resolution)

    slope_rad = np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))

    if degrees:
        return np.degrees(slope_rad).astype(np.float32)
    return slope_rad.astype(np.float32)


def compute_curvature(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    curvature_type: str = "profile",
) -> NDArray[np.float32]:
    """Compute curvature using Zevenbergen & Thorne (1987) method.

    Args:
        curvature_type: "profile" (in slope direction), "plan" (perpendicular),
                        or "total" (combined)

    Profile curvature: negative = concave (depressions), positive = convex (ridges)
    """
    L = resolution
    padded = np.pad(dem, 1, mode="edge")

    z = padded[1:-1, 1:-1]
    z_n = padded[:-2, 1:-1]
    z_s = padded[2:, 1:-1]
    z_e = padded[1:-1, 2:]
    z_w = padded[1:-1, :-2]
    z_ne = padded[:-2, 2:]
    z_nw = padded[:-2, :-2]
    z_se = padded[2:, 2:]
    z_sw = padded[2:, :-2]

    # Zevenbergen & Thorne parameters
    D = ((z_w + z_e) / 2 - z) / (L * L)
    E = ((z_n + z_s) / 2 - z) / (L * L)
    F = (-z_nw + z_ne + z_sw - z_se) / (4 * L * L)
    G = (-z_w + z_e) / (2 * L)
    H = (z_n - z_s) / (2 * L)

    if curvature_type == "profile":
        # Profile curvature (in direction of maximum slope)
        denom = (G**2 + H**2) * (1 + G**2 + H**2) ** 1.5
        # Avoid division by zero
        with np.errstate(divide="ignore", invalid="ignore"):
            curv = -2 * (D * G**2 + E * H**2 + F * G * H) / np.where(
                denom > 1e-10, denom, 1e-10
            )
        curv = np.where(np.isfinite(curv), curv, 0)

    elif curvature_type == "plan":
        # Plan curvature (perpendicular to slope)
        denom = (G**2 + H**2) ** 1.5
        with np.errstate(divide="ignore", invalid="ignore"):
            curv = 2 * (D * H**2 - F * G * H + E * G**2) / np.where(
                denom > 1e-10, denom, 1e-10
            )
        curv = np.where(np.isfinite(curv), curv, 0)

    else:  # total / mean
        curv = -2 * (D + E)

    return curv.astype(np.float32)


def compute_tpi(
    dem: NDArray[np.float32],
    radius_pixels: int = 15,
) -> NDArray[np.float32]:
    """Compute Topographic Position Index at a given radius.

    TPI = elevation - mean(elevation in annular neighborhood)
    Negative TPI = depression, positive = ridge/hilltop.

    Args:
        radius_pixels: neighborhood radius in pixels
    """
    kernel_size = 2 * radius_pixels + 1
    mean_elevation = uniform_filter(dem.astype(np.float64), size=kernel_size).astype(np.float32)
    return (dem - mean_elevation).astype(np.float32)


def compute_tpi_multiscale(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    radii_m: tuple[float, ...] = (5.0, 15.0, 50.0),
) -> dict[str, NDArray[np.float32]]:
    """Compute TPI at multiple scales.

    Returns dict mapping "tpi_{radius}m" to the TPI array.
    """
    results = {}
    for radius_m in radii_m:
        radius_px = max(1, int(round(radius_m / resolution)))
        key = f"tpi_{int(radius_m)}m"
        results[key] = compute_tpi(dem, radius_px)
    return results


def compute_svf(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    radius_m: float = 30.0,
    n_directions: int = 16,
) -> NDArray[np.float32]:
    """Compute Sky-View Factor.

    SVF = proportion of visible sky from each cell. Low SVF indicates
    concave/enclosed features (dolines, pits). Based on Zakšek et al. (2011).

    Args:
        radius_m: search radius in meters
        n_directions: number of azimuth directions to sample
    """
    rows, cols = dem.shape
    radius_px = max(1, int(round(radius_m / resolution)))
    svf = np.zeros_like(dem, dtype=np.float64)

    angles = np.linspace(0, 2 * np.pi, n_directions, endpoint=False)

    for angle in angles:
        dx = np.cos(angle)
        dy = np.sin(angle)

        max_elevation_angle = np.zeros_like(dem, dtype=np.float64)

        for step in range(1, radius_px + 1):
            # Offset coordinates
            offset_r = int(round(step * dy))
            offset_c = int(round(step * dx))

            if abs(offset_r) >= rows or abs(offset_c) >= cols:
                break

            # Shifted elevation
            shifted = np.roll(np.roll(dem, -offset_r, axis=0), -offset_c, axis=1)

            # Distance in meters
            dist = step * resolution

            # Elevation angle
            with np.errstate(divide="ignore", invalid="ignore"):
                elev_angle = np.arctan2(shifted - dem, dist)
                elev_angle = np.where(np.isfinite(elev_angle), elev_angle, 0)

            max_elevation_angle = np.maximum(max_elevation_angle, elev_angle)

        svf += np.cos(max_elevation_angle)

    svf /= n_directions
    return svf.astype(np.float32)


def compute_lrm(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    kernel_m: float = 100.0,
) -> NDArray[np.float32]:
    """Compute Local Relief Model.

    LRM = DEM - lowpass_filtered(DEM)
    Negative values = depressions/entrances. Positive = ridges/mounds.

    Gold standard for cave entrance detection (Moyes & Montgomery 2019).

    Args:
        kernel_m: smoothing kernel radius in meters
    """
    kernel_px = max(3, int(round(kernel_m / resolution)))
    if kernel_px % 2 == 0:
        kernel_px += 1  # ensure odd

    smoothed = uniform_filter(dem.astype(np.float64), size=kernel_px).astype(np.float32)
    return (dem - smoothed).astype(np.float32)


def compute_lrm_multiscale(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
    kernel_sizes_m: tuple[float, ...] = (50.0, 100.0, 200.0),
) -> dict[str, NDArray[np.float32]]:
    """Compute LRM at multiple kernel sizes.

    Returns dict mapping "lrm_{kernel}m" to the LRM array.
    For cave detection, take per-pixel minimum across scales.
    """
    results = {}
    for kernel_m in kernel_sizes_m:
        key = f"lrm_{int(kernel_m)}m"
        results[key] = compute_lrm(dem, resolution, kernel_m)
    return results


def compute_fill_difference(
    dem: NDArray[np.float32],
) -> NDArray[np.float32]:
    """Compute fill-difference raster: filled_DEM - original_DEM.

    Positive values indicate depression depth at each pixel.
    Uses the priority-flood algorithm from the fill_difference pass.
    """
    from magic_eyes.detection.passes.fill_difference import _fill_depressions

    filled = _fill_depressions(dem)
    return (filled - dem).astype(np.float32)


def compute_all_derivatives(
    dem: NDArray[np.float32],
    resolution: float = 1.0,
) -> dict[str, NDArray[np.float32]]:
    """Compute all terrain derivatives from a DEM in parallel.

    Uses ThreadPoolExecutor — numpy/scipy release the GIL so threads
    give real parallelism across all CPU cores.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Define all independent computation tasks
    tasks = {
        "hillshade": lambda: compute_hillshade(dem, resolution),
        "slope": lambda: compute_slope(dem, resolution),
        "profile_curvature": lambda: compute_curvature(dem, resolution, "profile"),
        "plan_curvature": lambda: compute_curvature(dem, resolution, "plan"),
        "svf": lambda: compute_svf(dem, resolution, radius_m=30.0, n_directions=16),
        "tpi_5m": lambda: compute_tpi(dem, max(1, int(5 / resolution))),
        "tpi_15m": lambda: compute_tpi(dem, max(1, int(15 / resolution))),
        "tpi_50m": lambda: compute_tpi(dem, max(1, int(50 / resolution))),
        "lrm_50m": lambda: compute_lrm(dem, resolution, 50.0),
        "lrm_100m": lambda: compute_lrm(dem, resolution, 100.0),
        "lrm_200m": lambda: compute_lrm(dem, resolution, 200.0),
        "fill_difference": lambda: compute_fill_difference(dem),
    }

    derivatives: dict[str, NDArray[np.float32]] = {}

    with ThreadPoolExecutor(max_workers=min(len(tasks), 8)) as executor:
        futures = {executor.submit(fn): name for name, fn in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                derivatives[name] = future.result()
            except Exception as e:
                # Non-fatal: skip failed derivatives
                import structlog
                structlog.get_logger().warning("derivative_failed", name=name, error=str(e))

    return derivatives
