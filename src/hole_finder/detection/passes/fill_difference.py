"""Fill-difference detection pass — finds depressions from pre-computed fill-difference raster.

Consumes the fill_difference derivative (filled_DEM - original_DEM).
Does NOT compute fill-difference itself — that's done by the processing pipeline
using WhiteboxTools (compiled Rust) and GDAL.

Based on Wall et al. (2016) — 93% detection rate for known sinkholes.

Vectorized: uses scipy.ndimage bulk operations across all labels at once
instead of per-region Python loops. O(H*W) instead of O(N*H*W).
"""

import numpy as np
from rasterio.features import shapes as rasterio_shapes
from shapely.geometry import Point, shape

from hole_finder.detection.array_backend import label, region_stats
from hole_finder.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from hole_finder.detection.registry import register_pass


@register_pass
class FillDifferencePass(DetectionPass):
    """Detect depressions from pre-computed fill-difference raster."""

    @property
    def name(self) -> str:
        return "fill_difference"

    @property
    def version(self) -> str:
        return "0.3.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["fill_difference"]

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        min_depth_m = config.get("min_depth_m", 0.5)
        max_area_m2 = config.get("max_area_m2", 5000.0)
        min_area_m2 = config.get("min_area_m2", 25.0)

        resolution = abs(input_data.transform[0])
        cell_area = resolution * resolution

        diff = input_data.derivatives.get("fill_difference")
        if diff is None:
            return []

        # Mask nodata (bogus huge values from DEM edges)
        diff = np.where(np.isfinite(diff) & (diff < 1000), diff, 0)

        depression_mask = diff > min_depth_m
        if not np.any(depression_mask):
            return []

        labeled, num_features = label(depression_mask)
        if num_features == 0:
            return []

        # Vectorized bulk stats — GPU if available, CPU otherwise
        stats = region_stats(diff, labeled, num_features, mask=depression_mask.astype(np.float32))
        areas_px = stats["areas_px"]
        areas_m2 = areas_px * cell_area
        max_depths = stats["max_vals"]
        centroids = stats["centroids"]

        # Filter by area bounds (vectorized)
        valid = (areas_m2 >= min_area_m2) & (areas_m2 <= max_area_m2)

        # Pre-compute outline polygons for all valid regions in one pass
        # rasterio_shapes vectorizes labeled array → polygons in raster CRS
        outlines: dict[int, object] = {}
        try:
            for geom_dict, value in rasterio_shapes(
                labeled.astype(np.int32),
                mask=(labeled > 0),
                transform=input_data.transform,
            ):
                label_id = int(value)
                # label IDs are 1-indexed, our arrays are 0-indexed
                arr_idx = label_id - 1
                if arr_idx in np.flatnonzero(valid):
                    outlines[arr_idx] = shape(geom_dict)
        except Exception:
            pass  # outline extraction is best-effort

        candidates = []
        for idx in np.flatnonzero(valid):
            cy, cx = centroids[idx]
            geo_x, geo_y = input_data.transform * (float(cx), float(cy))
            depth = float(max_depths[idx])

            candidates.append(
                Candidate(
                    geometry=Point(geo_x, geo_y),
                    outline=outlines.get(idx),
                    score=min(depth / 5.0, 1.0),
                    feature_type=FeatureType.DEPRESSION,
                    morphometrics={
                        "depth_m": depth,
                        "area_m2": float(areas_m2[idx]),
                        "area_pixels": float(areas_px[idx]),
                    },
                )
            )

        return candidates
