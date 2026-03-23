"""Feature type classification based on morphometric properties."""

from hole_finder.detection.base import Candidate, FeatureType


def classify_candidate(candidate: Candidate) -> FeatureType:
    """Assign a feature type based on morphometric properties.

    Uses simple heuristic rules. ML-based classification comes later.
    """
    m = candidate.morphometrics
    depth = m.get("depth_m", 0)
    area = m.get("area_m2", 0)
    circularity = m.get("circularity", 0)

    # Very large, shallow, irregular = salt dome collapse (LA)
    if area > 10000 and depth < 5.0 and circularity < 0.3:
        return FeatureType.SALT_DOME_COLLAPSE

    # Elongated, moderate depth, large = lava tube collapse (CA)
    if circularity < 0.2 and depth > 0.5 and area > 500:
        return FeatureType.LAVA_TUBE

    # Very deep, small, circular = likely cave entrance or pit
    if depth > 3.0 and area < 500 and circularity > 0.4:
        return FeatureType.CAVE_ENTRANCE

    # Rectangular/low circularity with moderate depth = mine portal
    if circularity < 0.3 and depth > 1.0 and area < 1000:
        return FeatureType.MINE_PORTAL

    # Large, shallow, circular = sinkhole
    if area > 200 and depth > 0.5 and circularity > 0.5:
        return FeatureType.SINKHOLE

    # Moderate features = generic depression
    if depth > 0.5:
        return FeatureType.DEPRESSION

    return FeatureType.UNKNOWN
