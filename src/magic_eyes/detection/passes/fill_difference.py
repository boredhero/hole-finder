"""Fill-difference detection pass: subtract DEM from filled DEM to find depressions.

Based on Wall et al. (2016) — achieves 93% detection rate for known sinkholes.
"""

import numpy as np
from scipy.ndimage import label as ndimage_label

from magic_eyes.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from magic_eyes.detection.postprocess.clustering import extract_candidates_from_labels
from magic_eyes.detection.registry import register_pass


def _fill_depressions(dem: np.ndarray) -> np.ndarray:
    """Fill all depressions in a DEM using priority-flood algorithm.

    Based on Barnes et al. (2014) priority-flood. Initializes all edge cells
    in a min-heap, then floods inward, raising any cell lower than its
    pour-point neighbor.
    """
    import heapq

    rows, cols = dem.shape
    filled = dem.copy()
    visited = np.zeros((rows, cols), dtype=bool)
    heap: list[tuple[float, int, int]] = []

    # Seed the heap with all border cells
    for r in range(rows):
        for c in range(cols):
            if r == 0 or r == rows - 1 or c == 0 or c == cols - 1:
                heapq.heappush(heap, (float(dem[r, c]), r, c))
                visited[r, c] = True

    # 8-connected neighbors
    neighbors = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]

    while heap:
        elev, r, c = heapq.heappop(heap)
        for dr, dc in neighbors:
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols and not visited[nr, nc]:
                visited[nr, nc] = True
                if filled[nr, nc] < elev:
                    filled[nr, nc] = elev
                heapq.heappush(heap, (float(filled[nr, nc]), nr, nc))

    return filled


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
