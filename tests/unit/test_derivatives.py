"""Tests for terrain derivatives — uses real GDAL + WhiteboxTools.

Every test writes a GeoTIFF, runs the native pipeline, reads the result.
Skipped if GDAL/WBT not available. Runs on .111 and in Docker.
"""

import shutil
import tempfile
from pathlib import Path

import numpy as np
import pytest
import rasterio

from tests.fixtures.synthetic_dem import (
    make_flat_geotiff,
    make_sinkhole_geotiff,
    make_slope_geotiff,
    write_geotiff,
)

GDAL_AVAILABLE = shutil.which("gdaldem") is not None
WBT_AVAILABLE = True
try:
    import whitebox
except Exception:
    WBT_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not GDAL_AVAILABLE or not WBT_AVAILABLE,
    reason="Requires GDAL + WhiteboxTools",
)


def _run_pipeline(dem_path: Path, tmpdir: Path) -> dict[str, Path]:
    from magic_eyes.processing.pipeline import ProcessingPipeline
    result = ProcessingPipeline(output_dir=tmpdir / "out").process_dem_file(dem_path, force=True)
    return result.derivative_paths


def _read(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1)


# --- Hillshade ---

class TestHillshade:
    def test_flat_terrain_uniform(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d), d)
            hs = _read(derivs["hillshade"])
            assert np.std(hs[hs > 0]) < 5.0

    def test_output_range(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d), d)
            hs = _read(derivs["hillshade"])
            assert hs.min() >= 0
            assert hs.max() <= 255

    def test_shape_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d, size=80), d)
            assert _read(derivs["hillshade"]).shape == (80, 80)


# --- Slope ---

class TestSlope:
    def test_flat_zero_slope(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d), d)
            slope = _read(derivs["slope"])
            assert np.mean(slope) < 1.0

    def test_sloped_terrain_nonzero(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_slope_geotiff(d, slope_deg=15.0), d)
            slope = _read(derivs["slope"])
            assert np.mean(slope[10:-10, 10:-10]) > 5.0


# --- Curvature ---

class TestCurvature:
    def test_flat_near_zero(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d), d)
            curv = _read(derivs["profile_curvature"])
            assert np.mean(np.abs(curv)) < 0.01

    def test_pit_has_curvature(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, depth=5.0, radius=15.0), d)
            curv = _read(derivs["profile_curvature"])
            assert np.max(np.abs(curv)) > 0.001

    def test_plan_curvature_exists(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d), d)
            assert "plan_curvature" in derivs
            assert _read(derivs["plan_curvature"]).shape[0] > 0


# --- TPI ---

class TestTPI:
    def test_flat_near_zero(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d), d)
            tpi = _read(derivs["tpi"])
            # GDAL TPI outputs nodata (-9999) on edges, mask those
            valid = tpi[(tpi > -9000) & (tpi < 9000)]
            assert np.mean(np.abs(valid)) < 0.1

    def test_pit_negative_tpi(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, depth=5.0, radius=15.0), d)
            tpi = _read(derivs["tpi"])
            assert tpi.min() < -0.5

    def test_mound_positive_tpi(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            dem = np.full((200, 200), 500.0, dtype=np.float32)
            y, x = np.mgrid[0:200, 0:200].astype(np.float32)
            dist = np.sqrt((x - 100) ** 2 + (y - 100) ** 2)
            dem[dist < 15] = 505.0
            dem_path = write_geotiff(d / "mound.tif", dem)
            derivs = _run_pipeline(dem_path, d)
            tpi = _read(derivs["tpi"])
            assert tpi.max() > 0.5


# --- SVF ---

class TestSVF:
    def test_flat_high_svf(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d, size=100), d)
            svf = _read(derivs["svf"])
            assert np.mean(svf[20:-20, 20:-20]) > 0.8

    def test_pit_lower_svf(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, depth=8.0, radius=15.0, size=100), d)
            svf = _read(derivs["svf"])
            center = svf[40:60, 40:60].mean()
            edge = svf[5:15, 5:15].mean()
            assert center < edge

    def test_output_range(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, size=100), d)
            svf = _read(derivs["svf"])
            assert svf.min() >= 0
            assert svf.max() <= 1.1


# --- LRM ---

class TestLRM:
    def test_flat_near_zero(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d), d)
            lrm = _read(derivs["lrm_100m"])
            assert np.mean(np.abs(lrm)) < 1.0

    def test_pit_negative_lrm(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, depth=5.0, radius=15.0), d)
            lrm = _read(derivs["lrm_50m"])
            assert lrm.min() < -1.0

    def test_multiscale(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d), d)
            assert "lrm_50m" in derivs
            assert "lrm_100m" in derivs
            assert "lrm_200m" in derivs


# --- Fill-Difference ---

class TestFillDifference:
    def test_flat_zero(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_flat_geotiff(d), d)
            fd = _read(derivs["fill_difference"])
            assert fd.max() < 0.01

    def test_pit_positive(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, depth=5.0), d)
            fd = _read(derivs["fill_difference"])
            assert fd.max() > 1.0

    def test_slope_zero(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_slope_geotiff(d), d)
            fd = _read(derivs["fill_difference"])
            assert fd.max() < 0.5


# --- All Derivatives ---

class TestAllDerivatives:
    def test_all_returned(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            derivs = _run_pipeline(make_sinkhole_geotiff(d, size=100), d)
            expected = ["hillshade", "slope", "tpi", "svf", "fill_difference",
                        "lrm_50m", "lrm_100m", "lrm_200m", "profile_curvature", "plan_curvature"]
            for name in expected:
                assert name in derivs, f"Missing derivative: {name}"
                assert derivs[name].exists(), f"{name} file doesn't exist"
