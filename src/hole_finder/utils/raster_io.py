"""Rasterio read/write helpers."""

from pathlib import Path

import numpy as np
import rasterio
from numpy.typing import NDArray
from rasterio.transform import Affine

from hole_finder.utils.crs import resolve_epsg


def read_dem(path: Path) -> tuple[NDArray[np.float32], Affine, int]:
    """Read a single-band DEM GeoTIFF.
    Returns (array, transform, epsg_code).
    """
    with rasterio.open(path) as src:
        dem = src.read(1).astype(np.float32)
        transform = src.transform
        crs = resolve_epsg(src.crs)
    return dem, transform, crs


def write_raster(
    path: Path,
    data: NDArray[np.float32],
    transform: Affine,
    crs: int,
    nodata: float | None = None,
) -> None:
    """Write a single-band float32 raster to GeoTIFF."""
    profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "width": data.shape[1],
        "height": data.shape[0],
        "count": 1,
        "crs": f"EPSG:{crs}",
        "transform": transform,
        "compress": "deflate",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)
