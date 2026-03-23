"""Morphometric filter pass — enriches candidates with full morphometrics.

Consumes pre-computed fill_difference and slope derivatives.
Never computes derivatives itself.

Vectorized: uses batch_morphometrics() to compute all 8 metrics for all
regions in a single set of scipy.ndimage passes, then filters vectorized.
"""

import numpy as np
from rasterio.features import shapes as rasterio_shapes
from scipy import ndimage
from shapely.geometry import Point, shape

from hole_finder.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from hole_finder.detection.postprocess.classification import classify_candidate
from hole_finder.detection.postprocess.morphometrics import batch_morphometrics
from hole_finder.detection.registry import register_pass


@register_pass
class MorphometricFilterPass(DetectionPass):

    @property
    def name(self) -> str:
        return "morphometric_filter"

    @property
    def version(self) -> str:
        return "0.3.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["fill_difference", "slope"]

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        min_depth_m = config.get("min_depth_m", 0.3)
        max_area_m2 = config.get("max_area_m2", 4000.0)
        min_area_m2 = config.get("min_area_m2", 25.0)
        min_circularity = config.get("min_circularity", 0.15)

        resolution = abs(input_data.transform[0])
        dem = input_data.dem

        fill_diff = input_data.derivatives.get("fill_difference")
        slope = input_data.derivatives.get("slope")
        if fill_diff is None or slope is None:
            return []

        # Mask nodata
        fill_diff = np.where(np.isfinite(fill_diff) & (fill_diff < 1000), fill_diff, 0)

        depression_mask = fill_diff > min_depth_m
        if not np.any(depression_mask):
            return []

        labeled, num_features = ndimage.label(depression_mask)
        if num_features == 0:
            return []

        # Batch-compute ALL morphometrics at once
        metrics = batch_morphometrics(dem, fill_diff, slope, labeled, num_features, resolution)

        # Vectorized filtering
        valid = (
            (metrics["area_m2"] >= min_area_m2)
            & (metrics["area_m2"] <= max_area_m2)
            & (metrics["circularity"] >= min_circularity)
            & (metrics["depth_m"] >= min_depth_m)
        )

        # Pre-compute outline polygons for all valid regions
        outlines: dict[int, object] = {}
        try:
            for geom_dict, value in rasterio_shapes(
                labeled.astype(np.int32),
                mask=(labeled > 0),
                transform=input_data.transform,
            ):
                arr_idx = int(value) - 1  # label IDs are 1-indexed
                if arr_idx in np.flatnonzero(valid):
                    outlines[arr_idx] = shape(geom_dict)
        except Exception:
            pass  # outline extraction is best-effort

        candidates = []
        for idx in np.flatnonzero(valid):
            cy, cx = metrics["centroids"][idx]
            geo_x, geo_y = input_data.transform * (float(cx), float(cy))

            depth = float(metrics["depth_m"][idx])
            circ = float(metrics["circularity"][idx])
            area = float(metrics["area_m2"][idx])

            depth_score = min(depth / 5.0, 1.0)
            score = (depth_score + circ) / 2.0

            candidate = Candidate(
                geometry=Point(geo_x, geo_y),
                outline=outlines.get(idx),
                score=score,
                feature_type=FeatureType.UNKNOWN,
                morphometrics={
                    "depth_m": depth,
                    "area_m2": area,
                    "perimeter_m": float(metrics["perimeter_m"][idx]),
                    "circularity": circ,
                    "volume_m3": float(metrics["volume_m3"][idx]),
                    "k_parameter": float(metrics["k_parameter"][idx]),
                    "elongation": float(metrics["elongation"][idx]),
                    "wall_slope_deg": float(metrics["wall_slope_deg"][idx]),
                    "depth_area_ratio": depth / area if area > 0 else 0,
                },
            )
            candidate.feature_type = classify_candidate(candidate)
            candidates.append(candidate)

        return candidates
