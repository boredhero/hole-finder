"""Curvature detection pass — consumes pre-computed curvature rasters.

Vectorized: uses scipy.ndimage bulk operations across all labels at once.
"""

import numpy as np
from shapely.geometry import Point

from hole_finder.detection.array_backend import label, region_stats
from hole_finder.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from hole_finder.detection.registry import register_pass


@register_pass
class CurvaturePass(DetectionPass):

    @property
    def name(self) -> str:
        return "curvature"

    @property
    def version(self) -> str:
        return "0.3.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["profile_curvature"]

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        threshold = config.get("threshold", -0.02)
        min_area_pixels = config.get("min_area_pixels", 4)

        resolution = abs(input_data.transform[0])
        cell_area = resolution * resolution

        curv = input_data.derivatives.get("profile_curvature")
        if curv is None:
            return []

        concave_mask = curv < threshold
        if not np.any(concave_mask):
            return []

        labeled, num_features = label(concave_mask)
        if num_features == 0:
            return []

        stats = region_stats(curv, labeled, num_features, mask=concave_mask.astype(np.float32))
        areas_px = stats["areas_px"]
        min_curvs = stats["min_vals"]
        centroids = stats["centroids"]

        valid = areas_px >= min_area_pixels

        candidates = []
        for idx in np.flatnonzero(valid):
            cy, cx = centroids[idx]
            geo_x, geo_y = input_data.transform * (float(cx), float(cy))
            min_curv_val = float(min_curvs[idx])
            strength = min(abs(min_curv_val) / 0.1, 1.0)
            area_m2 = float(areas_px[idx]) * cell_area

            candidates.append(
                Candidate(
                    geometry=Point(geo_x, geo_y),
                    score=strength,
                    feature_type=FeatureType.DEPRESSION,
                    morphometrics={"min_curvature": min_curv_val, "area_m2": area_m2},
                )
            )

        return candidates
