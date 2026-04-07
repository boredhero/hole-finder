"""Pass runner: orchestrates detection pass chains on tiles."""

import time
from pathlib import Path
from typing import Any

import numpy as np
import rasterio

from hole_finder.detection.base import Candidate, DetectionPass, PassInput
from hole_finder.detection.fusion import ResultFuser
from hole_finder.detection.registry import PassRegistry
from hole_finder.utils.crs import resolve_epsg
from hole_finder.utils.logging import log
from hole_finder.utils.perf import get_profiler


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
        profiler = get_profiler()

        t0 = time.perf_counter()
        with rasterio.open(dem_path) as src:
            dem = src.read(1).astype(np.float32)
            transform = src.transform
            crs = resolve_epsg(src.crs)
        dem_io_elapsed = time.perf_counter() - t0

        log.info(
            "raster_io_dem",
            elapsed_s=round(dem_io_elapsed, 3),
            shape=list(dem.shape),
            size_mb=round(dem.nbytes / 1e6, 1),
        )
        if profiler:
            profiler.record("load_dem", dem_io_elapsed, parent="detection_io")

        # Load derivative rasters
        loaded_derivatives: dict[str, np.ndarray] = {}
        if derivatives:
            t0 = time.perf_counter()
            total_bytes = 0
            for name, path in derivatives.items():
                with rasterio.open(path) as src:
                    arr = src.read(1).astype(np.float32)
                    loaded_derivatives[name] = arr
                    total_bytes += arr.nbytes
            deriv_io_elapsed = time.perf_counter() - t0
            log.info(
                "raster_io_derivatives",
                elapsed_s=round(deriv_io_elapsed, 3),
                count=len(loaded_derivatives),
                total_mb=round(total_bytes / 1e6, 1),
            )
            if profiler:
                profiler.record(
                    "load_derivatives", deriv_io_elapsed, parent="detection_io",
                    count=len(loaded_derivatives), total_mb=round(total_bytes / 1e6, 1),
                )

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
        the GIL, so threads give real parallelism for C-level operations.

        Thread safety: Each pass gets its own read-only view of the shared
        numpy arrays (numpy arrays are thread-safe for read operations).
        No pass mutates the input arrays. The profiler uses a threading.Lock
        internally for safe concurrent writes.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if derivatives is None:
            derivatives = {}

        profiler = get_profiler()
        detection_wall_start = time.perf_counter()

        log.info(
            "detection_start",
            passes=[p.name for p in self.passes],
            num_passes=len(self.passes),
            dem_shape=list(dem.shape),
            derivatives=list(derivatives.keys()),
            parallel=parallel,
        )

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
                t0 = time.perf_counter()
                candidates = detection_pass.run(pass_input)
                elapsed = time.perf_counter() - t0
                log.info(
                    "pass_complete",
                    pass_name=detection_pass.name,
                    candidates=len(candidates),
                    elapsed_s=round(elapsed, 3),
                    elapsed_ms=round(elapsed * 1000, 1),
                )
                if profiler:
                    profiler.record(
                        detection_pass.name, elapsed,
                        parent="detection_passes",
                        candidates=len(candidates),
                    )
                return [(detection_pass.name, c) for c in candidates]
            except Exception as e:
                elapsed = time.perf_counter() - t0
                log.warning(
                    "pass_failed",
                    pass_name=detection_pass.name,
                    error=str(e),
                    elapsed_s=round(elapsed, 3),
                )
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

        passes_elapsed = time.perf_counter() - detection_wall_start
        log.info(
            "all_passes_complete",
            wall_time_s=round(passes_elapsed, 3),
            total_raw_candidates=len(all_candidates),
        )

        # Fusion timing
        t0 = time.perf_counter()
        fused = self.fuser.fuse(all_candidates)
        fusion_elapsed = time.perf_counter() - t0

        total_elapsed = time.perf_counter() - detection_wall_start
        log.info(
            "detection_complete",
            total_s=round(total_elapsed, 3),
            passes_s=round(passes_elapsed, 3),
            fusion_s=round(fusion_elapsed, 3),
            raw_candidates=len(all_candidates),
            fused_candidates=len(fused),
        )
        if profiler:
            profiler.record("fusion", fusion_elapsed, parent="detection",
                            raw=len(all_candidates), fused=len(fused))
            profiler.record("detection_total", total_elapsed, parent=None,
                            passes=len(self.passes), fused=len(fused))

        return fused
