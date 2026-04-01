"""Local Relief Model detection pass — consumes pre-computed LRM rasters.

Gold standard for cave entrance detection (Moyes & Montgomery 2019).
LRM rasters are computed by WhiteboxTools in the processing pipeline.

Vectorized: uses scipy.ndimage bulk operations across all labels at once.
"""

import numpy as np
from rasterio.features import shapes as rasterio_shapes
from shapely.geometry import Point, shape

from hole_finder.detection.array_backend import label, region_stats
from hole_finder.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from hole_finder.detection.registry import register_pass


@register_pass
class LocalReliefModelPass(DetectionPass):

    @property
    def name(self) -> str:
        return "local_relief_model"

    @property
    def version(self) -> str:
        return "0.3.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["lrm_50m", "lrm_100m", "lrm_200m"]

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        threshold_m = config.get("threshold_m", 0.3)
        min_area_m2 = config.get("min_area_m2", 10.0)
        max_area_m2 = config.get("max_area_m2", 5000.0)

        resolution = abs(input_data.transform[0])
        cell_area = resolution * resolution

        # Combine multi-scale LRM — take per-pixel minimum (most negative = deepest anomaly)
        lrm_keys = [k for k in input_data.derivatives if k.startswith("lrm_")]
        if not lrm_keys:
            return []

        lrm_stack = [input_data.derivatives[k] for k in lrm_keys]
        combined = np.minimum.reduce(lrm_stack)

        depression_mask = combined < -threshold_m
        if not np.any(depression_mask):
            return []

        labeled, num_features = label(depression_mask)
        if num_features == 0:
            return []

        # Vectorized bulk stats — GPU if available
        stats = region_stats(combined, labeled, num_features, mask=depression_mask.astype(np.float32))
        areas_px = stats["areas_px"]
        areas_m2 = areas_px * cell_area
        anomaly_depths = -stats["min_vals"]
        centroids = stats["centroids"]

        valid = (areas_m2 >= min_area_m2) & (areas_m2 <= max_area_m2)

        valid_set = set(np.flatnonzero(valid).tolist())
        valid_labels = set(idx + 1 for idx in valid_set)
        masked_labeled = np.where(np.isin(labeled, list(valid_labels)), labeled, 0).astype(np.int32)

        outlines: dict[int, object] = {}
        for geom_dict, value in rasterio_shapes(masked_labeled, mask=(masked_labeled > 0), transform=input_data.transform):
            arr_idx = int(value) - 1
            outlines[arr_idx] = shape(geom_dict)

        candidates = []
        for idx in np.flatnonzero(valid):
            cy, cx = centroids[idx]
            geo_x, geo_y = input_data.transform * (float(cx), float(cy))
            depth = float(anomaly_depths[idx])

            candidates.append(
                Candidate(
                    geometry=Point(geo_x, geo_y),
                    outline=outlines.get(idx),
                    score=min(depth / 3.0, 1.0),
                    feature_type=FeatureType.CAVE_ENTRANCE,
                    morphometrics={"lrm_anomaly_m": depth, "area_m2": float(areas_m2[idx])},
                )
            )

        return candidates
