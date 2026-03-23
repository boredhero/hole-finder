"""Fill-difference detection pass: subtract DEM from filled DEM to find depressions.

Based on Wall et al. (2016) — achieves 93% detection rate for known sinkholes.
"""

import numpy as np
from scipy.ndimage import label as ndimage_label

from magic_eyes.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from magic_eyes.detection.registry import register_pass


def _fill_depressions(dem: np.ndarray) -> np.ndarray:
    """Fill all depressions in a DEM.

    Uses WhiteboxTools (compiled Rust) when available for massive speedup.
    Falls back to scipy-based iterative approach for small DEMs / testing.
    """
    # Try WhiteboxTools first (100x+ faster on real data)
    try:
        return _fill_whitebox(dem)
    except Exception:
        pass

    # Fallback: scipy iterative fill (works without external deps, good for small DEMs)
    return _fill_scipy(dem)


def _fill_whitebox(dem: np.ndarray) -> np.ndarray:
    """Fill depressions using WhiteboxTools (compiled Rust, very fast)."""
    import tempfile

    import rasterio
    import whitebox
    from rasterio.transform import from_bounds

    wbt = whitebox.WhiteboxTools()
    wbt.set_verbose_mode(False)

    with tempfile.TemporaryDirectory() as tmpdir:
        in_path = f"{tmpdir}/dem.tif"
        out_path = f"{tmpdir}/filled.tif"

        # Write DEM to temp file
        h, w = dem.shape
        transform = from_bounds(0, 0, w, h, w, h)
        with rasterio.open(in_path, "w", driver="GTiff", height=h, width=w,
                           count=1, dtype="float32", transform=transform) as dst:
            dst.write(dem, 1)

        # Run WhiteboxTools fill_depressions
        wbt.fill_depressions(in_path, out_path)

        # Read result
        with rasterio.open(out_path) as src:
            return src.read(1).astype(np.float32)


def _fill_scipy(dem: np.ndarray) -> np.ndarray:
    """Fill depressions using scipy morphological reconstruction.

    Uses grey-scale morphological reconstruction (erosion from marker):
    marker = DEM with edges, interior set to max.
    Reconstruction lowers the marker until it touches the DEM surface,
    effectively filling all depressions to their pour-point.
    This is the standard algorithm from Vincent (1993).
    """
    from scipy.ndimage import grey_erosion

    rows, cols = dem.shape
    # Marker: edges at original elevation, interior at max
    marker = np.full_like(dem, dem.max())
    marker[0, :] = dem[0, :]
    marker[-1, :] = dem[-1, :]
    marker[:, 0] = dem[:, 0]
    marker[:, -1] = dem[:, -1]

    # Morphological reconstruction by erosion
    # Iterate until stable: marker = max(dem, erode(marker))
    footprint = np.ones((3, 3))
    for _ in range(rows + cols):
        eroded = grey_erosion(marker, footprint=footprint)
        new_marker = np.maximum(dem, eroded)
        if np.array_equal(new_marker, marker):
            break
        marker = new_marker

    return marker


@register_pass
class FillDifferencePass(DetectionPass):
    """Detect depressions by subtracting DEM from sink-filled DEM."""

    @property
    def name(self) -> str:
        return "fill_difference"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def required_derivatives(self) -> list[str]:
        return []

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        min_depth_m = config.get("min_depth_m", 0.5)
        max_area_m2 = config.get("max_area_m2", 5000.0)
        min_area_m2 = config.get("min_area_m2", 25.0)

        dem = input_data.dem

        # Handle nodata
        nodata_mask = np.isnan(dem) | (dem < -9000)
        if np.any(nodata_mask):
            dem = dem.copy()
            dem[nodata_mask] = np.nanmax(dem)

        # Fill depressions
        filled = _fill_depressions(dem)

        # Difference: positive values = depression depth
        diff = filled - dem

        # Threshold
        depression_mask = diff > min_depth_m

        if not np.any(depression_mask):
            return []

        # Label connected components
        labeled, num_features = ndimage_label(depression_mask)

        # Compute resolution from transform
        resolution = abs(input_data.transform[0])

        # Filter by area
        candidates = []
        for i in range(1, num_features + 1):
            region_mask = labeled == i
            area_pixels = np.sum(region_mask)
            area_m2 = area_pixels * resolution * resolution

            if area_m2 < min_area_m2 or area_m2 > max_area_m2:
                continue

            # Centroid
            rows, cols = np.where(region_mask)
            cy, cx = float(np.mean(rows)), float(np.mean(cols))
            geo_x, geo_y = input_data.transform * (cx, cy)

            # Depth
            depth = float(np.max(diff[region_mask]))

            from shapely.geometry import Point

            candidates.append(
                Candidate(
                    geometry=Point(geo_x, geo_y),
                    score=min(depth / 5.0, 1.0),
                    feature_type=FeatureType.DEPRESSION,
                    morphometrics={
                        "depth_m": depth,
                        "area_m2": area_m2,
                        "area_pixels": float(area_pixels),
                    },
                )
            )

        return candidates
