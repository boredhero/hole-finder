"""Morphometric computation for detected features.

Provides both per-region functions (for single-feature analysis) and
vectorized batch functions that compute metrics across ALL labeled
regions in a single pass using scipy.ndimage.

Batch functions are the fast path — used by MorphometricFilterPass.
Per-region functions are kept for other callers that need them.
"""

import numpy as np
from numpy.typing import NDArray
from scipy import ndimage

from hole_finder.detection.array_backend import region_stats as gpu_region_stats


# --- Per-region functions (original API, used by individual callers) ---

def compute_depth(dem: NDArray[np.float32], mask: NDArray[np.bool_]) -> float:
    """Compute depth of a depression from DEM and binary mask."""
    if not np.any(mask):
        return 0.0
    rim_elevation = np.max(dem[mask])
    floor_elevation = np.min(dem[mask])
    return float(rim_elevation - floor_elevation)


def compute_area(mask: NDArray[np.bool_], resolution_m: float) -> float:
    """Compute area in m^2 from a binary mask and pixel resolution."""
    return float(np.sum(mask) * resolution_m * resolution_m)


def compute_circularity(area_m2: float, perimeter_m: float) -> float:
    """Compute circularity index: 4*pi*area / perimeter^2.

    Perfect circle = 1.0, elongated shapes < 1.0.
    """
    if perimeter_m <= 0:
        return 0.0
    return (4.0 * np.pi * area_m2) / (perimeter_m * perimeter_m)


def compute_perimeter(mask: NDArray[np.bool_], resolution_m: float) -> float:
    """Estimate perimeter from binary mask using edge counting."""
    interior = ndimage.binary_erosion(mask)
    edge = mask & ~interior
    return float(np.sum(edge) * resolution_m)


def compute_k_parameter(area_m2: float, depth_m: float, volume_m3: float) -> float:
    """Compute Telbisz k parameter: (area * depth) / volume.

    k ≈ 1: cylinder, k ≈ 2: bowl/calotte, k ≈ 3: cone.
    """
    if volume_m3 <= 0:
        return 0.0
    return (area_m2 * depth_m) / volume_m3


def compute_volume(
    dem: NDArray[np.float32], mask: NDArray[np.bool_], resolution_m: float
) -> float:
    """Compute volume of depression below rim elevation."""
    if not np.any(mask):
        return 0.0
    rim_elevation = np.max(dem[mask])
    depths = rim_elevation - dem[mask]
    depths = np.maximum(depths, 0)
    cell_area = resolution_m * resolution_m
    return float(np.sum(depths) * cell_area)


def compute_wall_slope(
    slope: NDArray[np.float32], mask: NDArray[np.bool_]
) -> float:
    """Compute mean slope of depression interior walls (degrees)."""
    if not np.any(mask):
        return 0.0
    return float(np.mean(slope[mask]))


def compute_elongation(mask: NDArray[np.bool_]) -> float:
    """Compute elongation ratio: minor_axis / major_axis.

    1.0 = circular, < 1.0 = elongated.
    """
    coords = np.argwhere(mask)
    if len(coords) < 3:
        return 1.0

    # PCA to find major/minor axes
    centered = coords - coords.mean(axis=0)
    cov = np.cov(centered.T)
    eigenvalues = np.linalg.eigvalsh(cov)
    eigenvalues = np.sort(eigenvalues)[::-1]

    if eigenvalues[0] <= 0:
        return 1.0
    return float(np.sqrt(eigenvalues[1] / eigenvalues[0]))


# --- Vectorized batch functions (fast path for all labels at once) ---

def batch_morphometrics(
    dem: NDArray[np.float32],
    fill_diff: NDArray[np.float32],
    slope: NDArray[np.float32],
    labeled: NDArray[np.int32],
    num_features: int,
    resolution_m: float,
) -> dict[str, NDArray]:
    """Compute all morphometrics for ALL labeled regions in bulk.

    Uses scipy.ndimage vectorized operations — single pass per metric
    over the entire array, instead of N passes for N regions.

    Returns dict of metric_name → array[num_features], indexed by label-1.
    """
    cell_area = resolution_m * resolution_m

    # Use GPU-accelerated backend for bulk stats on DEM
    dem_stats = gpu_region_stats(dem, labeled, num_features)
    areas_px = dem_stats["areas_px"]
    areas_m2 = areas_px * cell_area
    dem_max = dem_stats["max_vals"]
    dem_min = dem_stats["min_vals"]
    depths = dem_max - dem_min

    # Volume: (max_i * area_px_i - sum_dem_i) * cell_area
    dem_sum = dem_stats["sum_vals"]
    volumes = (dem_max * areas_px - dem_sum) * cell_area
    volumes = np.maximum(volumes, 0.0)

    # Wall slope: mean slope per region (GPU-accelerated)
    slope_stats = gpu_region_stats(slope, labeled, num_features)
    wall_slopes = slope_stats["mean_vals"]

    # Perimeter: count edge pixels per region using erosion (CPU — binary_erosion not in CuPy)
    eroded = ndimage.binary_erosion(labeled > 0)
    edge_mask = (labeled > 0) & ~eroded
    labels_arr = np.arange(1, num_features + 1)
    perimeters_px = ndimage.sum(edge_mask, labeled, labels_arr).astype(np.float64)
    perimeters_m = perimeters_px * resolution_m

    # Circularity: 4*pi*area / perimeter^2
    with np.errstate(divide="ignore", invalid="ignore"):
        circularities = np.where(
            perimeters_m > 0,
            (4.0 * np.pi * areas_m2) / (perimeters_m ** 2),
            0.0,
        )

    # K parameter: (area * depth) / volume
    with np.errstate(divide="ignore", invalid="ignore"):
        k_params = np.where(volumes > 0, (areas_m2 * depths) / volumes, 0.0)

    # Elongation via bounding-box aspect ratio (fast approximation).
    # True PCA elongation requires per-region coordinate extraction which
    # is expensive. BB ratio is a good proxy and stays vectorized.
    slices = ndimage.find_objects(labeled)
    elongations = np.ones(num_features, dtype=np.float64)
    for i, sl in enumerate(slices):
        if sl is not None:
            h = sl[0].stop - sl[0].start
            w = sl[1].stop - sl[1].start
            if max(h, w) > 0:
                elongations[i] = min(h, w) / max(h, w)

    # Centroids (already computed by GPU-accelerated backend)
    centroids = dem_stats["centroids"]

    return {
        "area_px": areas_px,
        "area_m2": areas_m2,
        "depth_m": depths,
        "volume_m3": volumes,
        "wall_slope_deg": wall_slopes,
        "perimeter_m": perimeters_m,
        "circularity": circularities,
        "k_parameter": k_params,
        "elongation": elongations,
        "centroids": centroids,
    }
