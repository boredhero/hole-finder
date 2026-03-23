"""TPI detection pass — consumes pre-computed TPI raster."""

import numpy as np
from scipy.ndimage import label as ndimage_label
from shapely.geometry import Point

from magic_eyes.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from magic_eyes.detection.registry import register_pass


@register_pass
class TPIPass(DetectionPass):

    @property
    def name(self) -> str:
        return "tpi"

    @property
    def version(self) -> str:
        return "0.2.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["tpi"]

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        threshold = config.get("threshold", -1.0)
        min_area_pixels = config.get("min_area_pixels", 4)

        resolution = abs(input_data.transform[0])

        tpi = input_data.derivatives.get("tpi")
        if tpi is None:
            return []

        # Mask nodata (GDAL TPI outputs -9999 on edges)
        tpi = np.where(np.isfinite(tpi) & (tpi > -9000), tpi, 0)
        depression_mask = tpi < threshold
        if not np.any(depression_mask):
            return []

        labeled, num_features = ndimage_label(depression_mask)

        candidates = []
        for i in range(1, num_features + 1):
            region = labeled == i
            if np.sum(region) < min_area_pixels:
                continue

            rows, cols = np.where(region)
            cy, cx = float(np.mean(rows)), float(np.mean(cols))
            geo_x, geo_y = input_data.transform * (cx, cy)

            min_tpi = float(np.min(tpi[region]))
            score = min(abs(min_tpi) / 5.0, 1.0)
            area_m2 = float(np.sum(region)) * resolution * resolution

            candidates.append(
                Candidate(
                    geometry=Point(geo_x, geo_y),
                    score=score,
                    feature_type=FeatureType.SINKHOLE,
                    morphometrics={"min_tpi": min_tpi, "area_m2": area_m2},
                )
            )

        return candidates
