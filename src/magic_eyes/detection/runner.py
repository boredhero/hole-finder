"""Pass runner: orchestrates detection pass chains on tiles."""

from pathlib import Path
from typing import Any

import numpy as np
import rasterio

from magic_eyes.detection.base import Candidate, DetectionPass, PassInput
from magic_eyes.detection.fusion import ResultFuser
from magic_eyes.detection.registry import PassRegistry


class PassRunner:
    """Executes a configured chain of detection passes on a tile."""

    def __init__(
        self,
        pass_names: list[str],
        config: dict[str, Any] | None = None,
        weights: dict[str, float] | None = None,
        min_confidence: float = 0.3,
    ):
        self.passes = PassRegistry.get_pass_chain(pass_names)
        self.config = config or {}
        self.fuser = ResultFuser(weights=weights, min_confidence=min_confidence)

    @classmethod
    def from_toml(cls, toml_path: Path) -> "PassRunner":
        """Create a PassRunner from a TOML configuration file."""
        import tomllib

        with open(toml_path, "rb") as f:
            data = tomllib.load(f)

        pipeline = data.get("pipeline", {})
        pass_names = pipeline.get("passes", [])
        min_confidence = pipeline.get("min_confidence", 0.3)
        weights = data.get("weights", {})

        # Flatten pass configs: {"passes": {"fill_difference": {...}}} → {"passes.fill_difference": {...}}
        config = {}
        for pass_name, pass_config in data.get("passes", {}).items():
            config[f"passes.{pass_name}"] = pass_config

        return cls(
            pass_names=pass_names,
            config=config,
            weights=weights,
            min_confidence=min_confidence,
        )

    def run_on_dem(
        self,
        dem_path: Path,
        derivatives: dict[str, Path] | None = None,
        point_cloud: Any | None = None,
    ) -> list[Candidate]:
        """Run all passes on a DEM file and return fused candidates."""
        with rasterio.open(dem_path) as src:
            dem = src.read(1).astype(np.float32)
            transform = src.transform
            crs = src.crs.to_epsg() or 32617

        # Load derivative rasters
        loaded_derivatives: dict[str, np.ndarray] = {}
        if derivatives:
            for name, path in derivatives.items():
                with rasterio.open(path) as src:
                    loaded_derivatives[name] = src.read(1).astype(np.float32)

        return self.run_on_array(dem, transform, crs, loaded_derivatives, point_cloud)

    def run_on_array(
        self,
        dem: np.ndarray,
        transform: Any,
        crs: int,
        derivatives: dict[str, np.ndarray] | None = None,
        point_cloud: Any | None = None,
        parallel: bool = True,
    ) -> list[Candidate]:
        """Run all passes on in-memory arrays and return fused candidates.

        When parallel=True (default), runs independent passes concurrently
        using a thread pool. Each pass is numpy/scipy-bound which releases
        the GIL, so threads give real parallelism.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if derivatives is None:
            derivatives = {}

        def _run_single_pass(detection_pass: DetectionPass) -> list[tuple[str, Candidate]]:
            pass_config = self.config.get(f"passes.{detection_pass.name}", {})
            pass_input = PassInput(
                dem=dem,
                transform=transform,
                crs=crs,
                derivatives=derivatives,
                point_cloud=point_cloud if detection_pass.requires_point_cloud else None,
                config=pass_config,
            )
            try:
                candidates = detection_pass.run(pass_input)
                return [(detection_pass.name, c) for c in candidates]
            except Exception as e:
                from magic_eyes.utils.logging import log
                log.warning("pass_failed", pass_name=detection_pass.name, error=str(e))
                return []

        all_candidates: list[tuple[str, Candidate]] = []

        if parallel and len(self.passes) > 1:
            with ThreadPoolExecutor(max_workers=min(len(self.passes), 8)) as executor:
                futures = {executor.submit(_run_single_pass, p): p for p in self.passes}
                for future in as_completed(futures):
                    all_candidates.extend(future.result())
        else:
            for detection_pass in self.passes:
                all_candidates.extend(_run_single_pass(detection_pass))

        return self.fuser.fuse(all_candidates)
