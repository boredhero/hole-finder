"""YOLOv8 object detection pass on hillshade images.

Detects bounding boxes around cave entrances and mine portals on
rendered hillshade images. Fine-tuned from yolov8m base.
Requires ultralytics package + GPU (ROCm or CUDA).
"""

from pathlib import Path

import numpy as np

from hole_finder.config import settings
from hole_finder.detection.base import Candidate, DetectionPass, FeatureType, PassInput
from hole_finder.detection.registry import register_pass
from hole_finder.utils.logging import log

# YOLO class index → our feature type
YOLO_CLASS_MAP = {
    0: FeatureType.SINKHOLE,
    1: FeatureType.CAVE_ENTRANCE,
    2: FeatureType.MINE_PORTAL,
    3: FeatureType.COLLAPSE_PIT,
}


@register_pass
class YOLODetectorPass(DetectionPass):
    """Object detection on hillshade images using YOLOv8."""

    @property
    def name(self) -> str:
        return "yolo_detector"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def required_derivatives(self) -> list[str]:
        return ["hillshade"]

    @property
    def requires_gpu(self) -> bool:
        return True

    def _load_model(self, model_path: Path | None = None):
        """Load trained YOLO model."""
        try:
            from ultralytics import YOLO
        except ImportError:
            return None

        if model_path is None:
            model_path = settings.models_dir / "yolo_terrain_v1.pt"

        if not model_path.exists():
            return None

        return YOLO(str(model_path))

    def run(self, input_data: PassInput) -> list[Candidate]:
        config = input_data.config
        confidence_threshold = config.get("confidence_threshold", 0.3)
        model_path = config.get("model_path")
        tile_size = config.get("tile_size", 640)

        model = self._load_model(Path(model_path) if model_path else None)
        if model is None:
            return []

        resolution = abs(input_data.transform[0])

        # Get hillshade
        hs = input_data.derivatives.get("hillshade")
        if hs is None:
            return []

        # Convert to 3-channel uint8 image (YOLO expects RGB)
        hs_norm = np.clip(hs, 0, 255).astype(np.uint8)
        img = np.stack([hs_norm, hs_norm, hs_norm], axis=-1)  # (H, W, 3)

        h, w = img.shape[:2]
        candidates = []

        # Tile the image for YOLO inference
        stride = tile_size - 64  # overlap
        for row in range(0, max(1, h - tile_size + 1), stride):
            for col in range(0, max(1, w - tile_size + 1), stride):
                patch = img[row:row + tile_size, col:col + tile_size]

                # Pad if needed
                ph, pw = patch.shape[:2]
                if ph < tile_size or pw < tile_size:
                    padded = np.zeros((tile_size, tile_size, 3), dtype=np.uint8)
                    padded[:ph, :pw] = patch
                    patch = padded

                # Run inference
                try:
                    results = model(patch, conf=confidence_threshold, verbose=False)
                except Exception as e:
                    log.debug("yolo_inference_failed", row=row, col=col, error=str(e))
                    continue

                for result in results:
                    boxes = result.boxes
                    if boxes is None:
                        continue

                    for box in boxes:
                        # Bounding box center in pixel coords (relative to patch)
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        cx_patch = (x1 + x2) / 2
                        cy_patch = (y1 + y2) / 2

                        # Convert to full-image pixel coords
                        cx_img = col + cx_patch
                        cy_img = row + cy_patch

                        # Convert to geographic coords
                        geo_x, geo_y = input_data.transform * (cx_img, cy_img)

                        cls_id = int(box.cls[0].item())
                        conf = float(box.conf[0].item())
                        feature_type = YOLO_CLASS_MAP.get(cls_id, FeatureType.UNKNOWN)

                        from shapely.geometry import Point

                        candidates.append(
                            Candidate(
                                geometry=Point(geo_x, geo_y),
                                score=conf,
                                feature_type=feature_type,
                                morphometrics={
                                    "bbox_width_px": float(x2 - x1),
                                    "bbox_height_px": float(y2 - y1),
                                    "bbox_area_m2": float((x2 - x1) * (y2 - y1) * resolution * resolution),
                                },
                                metadata={
                                    "classifier": "yolov8",
                                    "class_id": cls_id,
                                },
                            )
                        )

        return candidates
