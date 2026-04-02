"""Raster tile endpoints — serve hillshade, terrain-rgb, and composited terrain tiles.

Composited terrain tiles serve high-res LiDAR DEMs where available,
falling back to AWS Terrarium global tiles (~30m) elsewhere.
Uses a lazy cache: first request computes + caches, subsequent requests are instant.
"""

import asyncio
import io
import math
import os
import tempfile
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
# Shared httpx client — reuses TLS connections to AWS across all tile requests
_http_client: httpx.AsyncClient | None = None
# PNG minimum valid size (header + IHDR + IEND = ~67 bytes minimum)
_MIN_PNG_BYTES = 67

def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=15.0, limits=httpx.Limits(max_connections=20, max_keepalive_connections=10))
    return _http_client

def _atomic_write(path: Path, data: bytes) -> None:
    """Write bytes to path atomically via temp file + rename (prevents partial reads)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        os.write(fd, data)
        os.close(fd)
        os.rename(tmp, str(path))
    except Exception:
        os.close(fd) if not os.get_inheritable(fd) else None
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


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
    from pyproj import CRS as PyprojCRS, Transformer

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
                if not crs or crs.to_epsg() == 4326:
                    west, south, east, north = b.left, b.bottom, b.right, b.top
                else:
                    # Handle compound CRS (e.g. UTM + NAVD88 vertical) by
                    # extracting the horizontal component — pyproj returns inf
                    # when transforming directly from compound CRS WKT
                    epsg = crs.to_epsg()
                    if epsg:
                        src_crs = f"EPSG:{epsg}"
                    else:
                        pcrs = PyprojCRS(crs)
                        horiz = pcrs.sub_crs_list[0] if pcrs.sub_crs_list else pcrs
                        src_crs = f"EPSG:{horiz.to_epsg()}" if horiz.to_epsg() else horiz
                    transformer = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)
                    west, south = transformer.transform(b.left, b.bottom)
                    east, north = transformer.transform(b.right, b.top)
                    # Sanity check — inf means the transform failed silently
                    if not all(math.isfinite(v) for v in (west, south, east, north)):
                        log.warning("dem_bounds_infinite", path=str(dem_path), crs=str(crs)[:80])
                        continue
                bounds[str(dem_path)] = (west, south, east, north)
        except Exception as e:
            log.warning("dem_scan_failed", path=str(dem_path), error=str(e))

    log.info("dem_bounds_scanned", count=len(bounds))
    _dem_bounds_cache = bounds
    return bounds


def _find_dem_for_tile(west: float, south: float, east: float, north: float) -> str | None:
    """Find a processed DEM that overlaps the given WGS84 bbox.
    Returns the DEM with the most overlap (best coverage for this tile).
    rasterio.reproject handles partial coverage gracefully — pixels outside
    the DEM get nodata (0m elevation), so partial overlap is fine."""
    bounds = _scan_dem_bounds()
    best_path = None
    best_overlap = 0.0
    for path, (dw, ds, de, dn) in bounds.items():
        # Check bbox intersection
        ow = max(dw, west)
        os_ = max(ds, south)
        oe = min(de, east)
        on = min(dn, north)
        if ow < oe and os_ < on:
            overlap = (oe - ow) * (on - os_)
            if overlap > best_overlap:
                best_overlap = overlap
                best_path = path
    return best_path


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
    Atomic writes prevent MapLibre from reading partially-written PNGs (→ DOMException).
    """
    tile_path = settings.data_dir / "tile_cache" / "terrain" / str(z) / str(x) / f"{y}.png"
    # 1. Serve from cache — validate size to reject corrupt/partial files
    if tile_path.exists():
        data = tile_path.read_bytes()
        if len(data) >= _MIN_PNG_BYTES:
            return Response(content=data, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
        tile_path.unlink(missing_ok=True)
    # 2. Try LiDAR DEM
    bbox = _tile_to_bbox(z, x, y)
    dem_path = _find_dem_for_tile(*bbox)
    if dem_path:
        try:
            png_bytes = _render_terrain_tile_from_dem(dem_path, z, x, y)
            _atomic_write(tile_path, png_bytes)
            return Response(content=png_bytes, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
        except Exception as e:
            log.warning("terrain_render_failed", dem=dem_path, z=z, x=x, y=y, error=str(e))
    # 3. Proxy from AWS (shared client with connection pooling)
    try:
        resp = await _get_http_client().get(AWS_TERRAIN_URL.format(z=z, x=x, y=y))
        if resp.status_code == 200 and len(resp.content) >= _MIN_PNG_BYTES:
            _atomic_write(tile_path, resp.content)
            return Response(content=resp.content, media_type="image/png", headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        log.warning("aws_terrain_proxy_failed", z=z, x=x, y=y, error=str(e))
    # 4. Flat fallback — always valid, MapLibre can always decode this
    return Response(content=_make_flat_terrarium_png_256(), media_type="image/png", headers={"Cache-Control": "public, max-age=60"})


@router.post("/raster/terrain/warm")
async def warm_terrain_cache(
    west: float = Query(..., description="West longitude"),
    south: float = Query(..., description="South latitude"),
    east: float = Query(..., description="East longitude"),
    north: float = Query(..., description="North latitude"),
    min_zoom: int = Query(12, ge=0, le=20),
    max_zoom: int = Query(15, ge=0, le=20),
):
    """Pre-render and cache all terrain tiles for a bbox across zoom levels.

    Uses shared httpx client, atomic writes, and batched concurrent fetches
    (10 at a time) to avoid saturating the event loop or AWS connections.
    """
    cached = 0
    rendered = 0
    proxied = 0
    client = _get_http_client()
    sem = asyncio.Semaphore(5)
    async def _warm_one(z: int, x: int, y: int) -> str:
        tile_path = settings.data_dir / "tile_cache" / "terrain" / str(z) / str(x) / f"{y}.png"
        if tile_path.exists() and tile_path.stat().st_size >= _MIN_PNG_BYTES:
            return "cached"
        bbox = _tile_to_bbox(z, x, y)
        dem_path = _find_dem_for_tile(*bbox)
        if dem_path:
            try:
                png_bytes = _render_terrain_tile_from_dem(dem_path, z, x, y)
                _atomic_write(tile_path, png_bytes)
                return "rendered"
            except Exception:
                pass
        async with sem:
            try:
                resp = await client.get(AWS_TERRAIN_URL.format(z=z, x=x, y=y))
                if resp.status_code == 200 and len(resp.content) >= _MIN_PNG_BYTES:
                    _atomic_write(tile_path, resp.content)
                    return "proxied"
            except Exception:
                pass
        return "failed"
    tasks = []
    for z in range(min_zoom, max_zoom + 1):
        n = 2 ** z
        x_min = int((west + 180) / 360 * n)
        x_max = int((east + 180) / 360 * n)
        y_min = int((1 - math.log(math.tan(math.radians(north)) + 1 / math.cos(math.radians(north))) / math.pi) / 2 * n)
        y_max = int((1 - math.log(math.tan(math.radians(south)) + 1 / math.cos(math.radians(south))) / math.pi) / 2 * n)
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                tasks.append(_warm_one(z, x, y))
    results = await asyncio.gather(*tasks)
    cached = results.count("cached")
    rendered = results.count("rendered")
    proxied = results.count("proxied")
    log.info("terrain_cache_warmed", cached=cached, rendered=rendered, proxied=proxied, total=len(results))
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


@router.get("/raster/terrain/coverage")
async def get_terrain_coverage(
    west: float = Query(..., description="West longitude"),
    south: float = Query(..., description="South latitude"),
    east: float = Query(..., description="East longitude"),
    north: float = Query(..., description="North latitude"),
    z: int = Query(..., ge=0, le=20, description="Zoom level for tile grid"),
):
    """Return GeoJSON tile grid for the viewport with source metadata per tile.

    Each feature is a tile boundary polygon with a `source` property:
    - "lidar": tile is rendered from a custom LiDAR DEM
    - "aws": tile would be proxied from AWS Terrarium
    """
    n = 2 ** z
    x_min = max(0, int((west + 180) / 360 * n))
    x_max = min(n - 1, int((east + 180) / 360 * n))
    y_min_f = (1 - math.log(math.tan(math.radians(min(north, 85.05))) + 1 / math.cos(math.radians(min(north, 85.05)))) / math.pi) / 2 * n
    y_max_f = (1 - math.log(math.tan(math.radians(max(south, -85.05))) + 1 / math.cos(math.radians(max(south, -85.05)))) / math.pi) / 2 * n
    y_min = max(0, int(y_min_f))
    y_max = min(n - 1, int(y_max_f))
    # Cap at 200 tiles to avoid blowing up the response at low zoom
    tile_count = (x_max - x_min + 1) * (y_max - y_min + 1)
    if tile_count > 200:
        return {"type": "FeatureCollection", "features": []}
    features = []
    for tx in range(x_min, x_max + 1):
        for ty in range(y_min, y_max + 1):
            bbox = _tile_to_bbox(z, tx, ty)
            dem_path = _find_dem_for_tile(*bbox)
            source = "lidar" if dem_path else "aws"
            features.append({
                "type": "Feature",
                "properties": {"z": z, "x": tx, "y": ty, "source": source},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [bbox[0], bbox[1]], [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]], [bbox[0], bbox[3]],
                        [bbox[0], bbox[1]],
                    ]],
                },
            })
    return {"type": "FeatureCollection", "features": features}


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
