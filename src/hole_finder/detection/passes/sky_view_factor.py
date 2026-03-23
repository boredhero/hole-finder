"""Sky-View Factor detection pass — consumes pre-computed SVF raster.

Vectorized: uses scipy.ndimage bulk operations across all labels at once.
"""

import numpy as np
from shapely.geometry import Point

from hole_finder.detection.array_backend import label, region_stats
from hole_finder.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from hole_finder.detection.registry import register_pass


@register_pass
class SkyViewFactorPass(DetectionPass):

    @property
    def name(self) -> str:
        return "sky_view_factor"

    @property
    def version(self) -> str:
        return "0.3.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["svf"]

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        threshold = config.get("threshold", 0.75)
        min_area_pixels = config.get("min_area_pixels", 4)

        resolution = abs(input_data.transform[0])
        cell_area = resolution * resolution

        svf = input_data.derivatives.get("svf")
        if svf is None:
            return []

        enclosed_mask = svf < threshold
        if not np.any(enclosed_mask):
            return []

        labeled, num_features = label(enclosed_mask)
        if num_features == 0:
            return []

        # Vectorized bulk stats — GPU if available
        stats = region_stats(svf, labeled, num_features, mask=enclosed_mask.astype(np.float32))
        areas_px = stats["areas_px"]
        min_svfs = stats["min_vals"]
        centroids = stats["centroids"]

        valid = areas_px >= min_area_pixels

        candidates = []
        for idx in np.flatnonzero(valid):
            cy, cx = centroids[idx]
            geo_x, geo_y = input_data.transform * (float(cx), float(cy))
            min_svf_val = float(min_svfs[idx])
            score = min(max(0.0, 1.0 - min_svf_val), 1.0)
            area_m2 = float(areas_px[idx]) * cell_area

            candidates.append(
                Candidate(
                    geometry=Point(geo_x, geo_y),
                    score=score,
                    feature_type=FeatureType.DEPRESSION,
                    morphometrics={"min_svf": min_svf_val, "area_m2": area_m2},
                )
            )

        return candidates
