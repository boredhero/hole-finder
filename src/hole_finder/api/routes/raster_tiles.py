"""Raster tile endpoints — serve hillshade, terrain-rgb, and composited terrain tiles.

Composited terrain tiles serve high-res LiDAR DEMs where available,
falling back to AWS Terrarium global tiles (~30m) elsewhere.
Uses a lazy cache: first request computes + caches, subsequent requests are instant.
"""

import io
import math
from pathlib import Path

import httpx
import numpy as np
from fastapi import APIRouter, Query
from fastapi.responses import Response

from hole_finder.config import settings
from hole_finder.utils.logging import log

router = APIRouter(tags=["raster_tiles"])

# In-memory cache of processed DEM bounds: {path: (west, south, east, north)}
_dem_bounds_cache: dict[str, tuple[float, float, float, float]] | None = None
AWS_TERRAIN_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"


def _tile_to_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert ZXY tile coords to WGS84 bounding box."""
    n = 2 ** z
    lon_min = x / n * 360 - 180
    lon_max = (x + 1) / n * 360 - 180
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lon_min, lat_min, lon_max, lat_max


@router.get("/raster/{layer}/{z}/{x}/{y}.png")
async def get_raster_tile(
    layer: str,
    z: int,
    x: int,
    y: int,
):
    """Serve a raster tile as PNG.

    Supported layers: hillshade, slope, svf, lrm

    Tiles are served from pre-rendered cache. If not cached,
    returns 404 (tiles must be pre-generated during processing).
    """
    # Check tile cache
    cache_dir = settings.data_dir / "tile_cache" / layer / str(z) / str(x)
    tile_path = cache_dir / f"{y}.png"

    if tile_path.exists():
        return Response(
            content=tile_path.read_bytes(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    # Tile not cached — return transparent 1x1 PNG
    # (In production, we'd generate on-the-fly from the GeoTIFF)
    TRANSPARENT_PNG = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
        b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
        b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
        b"\r\n\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    return Response(
        content=TRANSPARENT_PNG,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=60"},
    )


def _scan_dem_bounds() -> dict[str, tuple[float, float, float, float]]:
    """Scan processed DEMs on disk and cache their WGS84 bounds.

    Returns dict mapping DEM file path → (west, south, east, north) in EPSG:4326.
    """
    global _dem_bounds_cache
    if _dem_bounds_cache is not None:
        return _dem_bounds_cache

    import rasterio
    from pyproj import Transformer

    bounds = {}
    processed_dir = settings.processed_dir
    if not processed_dir.exists():
        _dem_bounds_cache = bounds
        return bounds

    for dem_path in processed_dir.glob("*/*_dem.tif"):
        try:
            with rasterio.open(dem_path) as src:
                b = src.bounds
                crs = src.crs
                if crs and crs.to_epsg() != 4326:
                    transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
                    west, south = transformer.transform(b.left, b.bottom)
                    east, north = transformer.transform(b.right, b.top)
                else:
                    west, south, east, north = b.left, b.bottom, b.right, b.top
                bounds[str(dem_path)] = (west, south, east, north)
        except Exception as e:
            log.warning("dem_scan_failed", path=str(dem_path), error=str(e))

    log.info("dem_bounds_scanned", count=len(bounds))
    _dem_bounds_cache = bounds
    return bounds


def _find_dem_for_tile(west: float, south: float, east: float, north: float) -> str | None:
    """Find a processed DEM that covers the given WGS84 bbox."""
    bounds = _scan_dem_bounds()
    for path, (dw, ds, de, dn) in bounds.items():
        # Check overlap
        if dw <= west and ds <= south and de >= east and dn >= north:
            return path
    return None


def _render_terrain_tile_from_dem(dem_path: str, z: int, x: int, y: int) -> bytes:
    """Render a 256x256 Terrarium-encoded PNG from a LiDAR DEM GeoTIFF."""
    import rasterio
    from rasterio.warp import Resampling, reproject

    bbox = _tile_to_bbox(z, x, y)
    west, south, east, north = bbox

    # Target: 256x256 in EPSG:4326 (MapLibre terrain tiles use WGS84 bounds)
    from rasterio.transform import from_bounds
    dst_transform = from_bounds(west, south, east, north, 256, 256)

    with rasterio.open(dem_path) as src:
        dst_array = np.zeros((1, 256, 256), dtype=np.float32)
        reproject(
            source=rasterio.band(src, 1),
            destination=dst_array,
            dst_transform=dst_transform,
            dst_crs="EPSG:4326",
            resampling=Resampling.cubic,
        )

    elevation = dst_array[0]
    # Handle nodata
    elevation = np.nan_to_num(elevation, nan=0.0)

    # Terrarium encoding: elevation = (R * 256 + G + B / 256) - 32768
    encoded = elevation + 32768.0
    r = np.floor(encoded / 256).astype(np.uint8)
    g = np.floor(encoded % 256).astype(np.uint8)
    b = np.floor((encoded * 256) % 256).astype(np.uint8)

    # Create RGB PNG
    from PIL import Image
    img = Image.fromarray(np.stack([r, g, b], axis=-1), mode="RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@router.get("/raster/terrain/{z}/{x}/{y}.png")
async def get_composited_terrain_tile(z: int, x: int, y: int):
    """Serve composited terrain tiles — LiDAR where available, AWS Terrarium elsewhere.

    Uses Terrarium encoding: elevation = (R * 256 + G + B / 256) - 32768
    Lazy cache: first request computes + saves to disk, subsequent requests serve cached.
    """
    # 1. Check cache
    cache_dir = settings.data_dir / "tile_cache" / "terrain" / str(z) / str(x)
    tile_path = cache_dir / f"{y}.png"

    if tile_path.exists():
        return Response(
            content=tile_path.read_bytes(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    # 2. Check if we have a LiDAR DEM covering this tile
    bbox = _tile_to_bbox(z, x, y)
    dem_path = _find_dem_for_tile(*bbox)

    if dem_path:
        # 3a. Render from our high-res LiDAR DEM
        try:
            png_bytes = _render_terrain_tile_from_dem(dem_path, z, x, y)
            # Cache it
            cache_dir.mkdir(parents=True, exist_ok=True)
            tile_path.write_bytes(png_bytes)
            return Response(
                content=png_bytes,
                media_type="image/png",
                headers={"Cache-Control": "public, max-age=86400"},
            )
        except Exception as e:
            log.warning("terrain_render_failed", dem=dem_path, z=z, x=x, y=y, error=str(e))
            # Fall through to AWS

    # 3b. Proxy from AWS Terrarium tiles
    url = AWS_TERRAIN_URL.format(z=z, x=x, y=y)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                png_bytes = resp.content
                cache_dir.mkdir(parents=True, exist_ok=True)
                tile_path.write_bytes(png_bytes)
                return Response(
                    content=png_bytes,
                    media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"},
                )
    except Exception as e:
        log.warning("aws_terrain_proxy_failed", z=z, x=x, y=y, error=str(e))

    # 4. Fallback: 256x256 flat sea-level terrain tile
    # MapLibre raster-dem requires 256x256 — a 1x1 PNG breaks the decoder
    flat_png = _make_flat_terrarium_png_256()
    return Response(
        content=flat_png,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=60"},
    )


@router.post("/raster/terrain/warm")
async def warm_terrain_cache(
    west: float = Query(..., description="West longitude"),
    south: float = Query(..., description="South latitude"),
    east: float = Query(..., description="East longitude"),
    north: float = Query(..., description="North latitude"),
    min_zoom: int = Query(8, ge=0, le=20),
    max_zoom: int = Query(15, ge=0, le=20),
):
    """Pre-render and cache all terrain tiles for a bbox across zoom levels.

    Called after processing completes to warm the cache before the map loads.
    For a 3km radius area at z8-z15, this is typically 20-40 tiles.
    """
    import asyncio

    cached = 0
    rendered = 0
    proxied = 0

    for z in range(min_zoom, max_zoom + 1):
        n = 2 ** z
        # Convert bbox to tile range
        x_min = int((west + 180) / 360 * n)
        x_max = int((east + 180) / 360 * n)
        y_min = int((1 - math.log(math.tan(math.radians(north)) + 1 / math.cos(math.radians(north))) / math.pi) / 2 * n)
        y_max = int((1 - math.log(math.tan(math.radians(south)) + 1 / math.cos(math.radians(south))) / math.pi) / 2 * n)

        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                cache_dir = settings.data_dir / "tile_cache" / "terrain" / str(z) / str(x)
                tile_path = cache_dir / f"{y}.png"

                if tile_path.exists():
                    cached += 1
                    continue

                bbox = _tile_to_bbox(z, x, y)
                dem_path = _find_dem_for_tile(*bbox)

                if dem_path:
                    try:
                        png_bytes = _render_terrain_tile_from_dem(dem_path, z, x, y)
                        cache_dir.mkdir(parents=True, exist_ok=True)
                        tile_path.write_bytes(png_bytes)
                        rendered += 1
                        continue
                    except Exception:
                        pass

                # Proxy from AWS
                url = AWS_TERRAIN_URL.format(z=z, x=x, y=y)
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        resp = await client.get(url)
                        if resp.status_code == 200:
                            cache_dir.mkdir(parents=True, exist_ok=True)
                            tile_path.write_bytes(resp.content)
                            proxied += 1
                except Exception:
                    pass

    log.info("terrain_cache_warmed", cached=cached, rendered=rendered, proxied=proxied)
    return {"cached": cached, "rendered": rendered, "proxied": proxied}


_flat_terrain_cache: bytes | None = None


def _make_flat_terrarium_png_256() -> bytes:
    """Create a 256x256 Terrarium PNG encoding elevation = 0m.

    MapLibre's raster-dem decoder requires 256x256 tiles.
    Terrarium: 0m = (128 * 256 + 0 + 0/256) - 32768 = 0. So R=128, G=0, B=0.
    """
    global _flat_terrain_cache
    if _flat_terrain_cache is not None:
        return _flat_terrain_cache

    from PIL import Image
    img = Image.new("RGB", (256, 256), (128, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    _flat_terrain_cache = buf.getvalue()
    return _flat_terrain_cache


@router.get("/raster/terrain-rgb/{z}/{x}/{y}.png")
async def get_terrain_rgb_tile(
    z: int,
    x: int,
    y: int,
):
    """Serve terrain-RGB tiles for MapLibre 3D terrain.

    Terrain-RGB encodes elevation as: elevation = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
    """
    cache_dir = settings.processed_dir / "tile_cache" / "terrain-rgb" / str(z) / str(x)
    tile_path = cache_dir / f"{y}.png"

    if tile_path.exists():
        return Response(
            content=tile_path.read_bytes(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    # Not cached — return sea level terrain-rgb (elevation = 0)
    # RGB for 0m: R=1, G=134, B=160 (since 0 = -10000 + (1*65536 + 134*256 + 160) * 0.1)
    FLAT_PNG = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
        b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
        b"\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05"
        b"\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    return Response(
        content=FLAT_PNG,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=60"},
    )
