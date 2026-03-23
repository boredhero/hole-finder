"""FastAPI application factory."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: import passes to trigger registration
    import magic_eyes.detection.passes  # noqa: F401

    yield
    # Shutdown


def create_app() -> FastAPI:
    app = FastAPI(
        title="Magic Eyes",
        description="LiDAR terrain anomaly detection API — caves, mines, sinkholes",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://anomalies.martinospizza.dev",
            "http://localhost:5173",  # dev
            "http://localhost:8000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register all routes
    from magic_eyes.api.routes import (
        datasets,
        detections,
        exports,
        jobs,
        raster_tiles,
        regions,
        tiles,
        validation,
        websocket,
    )

    app.include_router(detections.router, prefix="/api")
    app.include_router(jobs.router, prefix="/api")
    app.include_router(datasets.router, prefix="/api")
    app.include_router(regions.router, prefix="/api")
    app.include_router(validation.router, prefix="/api")
    app.include_router(exports.router, prefix="/api")
    app.include_router(tiles.router, prefix="/api")
    app.include_router(raster_tiles.router, prefix="/api")
    app.include_router(websocket.router)

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "version": _load_info().get("version", "unknown")}

    @app.get("/api/info")
    async def info():
        return _load_info()

    # Serve built frontend as SPA
    _mount_frontend(app)

    return app


def _load_info() -> dict:
    """Load info.yml for version display."""
    from pathlib import Path

    info_candidates = [
        Path(__file__).parent.parent.parent / "info.yml",
        Path("/app/info.yml"),
    ]
    for p in info_candidates:
        if p.exists():
            # Simple YAML-subset parser (key: value lines)
            data = {}
            for line in p.read_text().splitlines():
                line = line.strip()
                if ":" in line and not line.startswith("#"):
                    key, _, val = line.partition(":")
                    val = val.strip()
                    # Try numeric conversion
                    try:
                        val = int(val)
                    except (ValueError, TypeError):
                        pass
                    data[key.strip()] = val
            return data
    return {"version": "0.1.0", "name": "Magic Eyes"}


def _mount_frontend(app: FastAPI) -> None:
    """Mount built frontend static files with SPA fallback."""
    from pathlib import Path

    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles

    # Check for static dir (Docker) then frontend/dist (dev)
    for candidate in [
        Path(__file__).parent.parent.parent / "static",
        Path(__file__).parent.parent.parent / "frontend" / "dist",
    ]:
        if (candidate / "index.html").exists():
            static_dir = candidate
            break
    else:
        return  # no frontend built

    # Mount /assets for hashed JS/CSS bundles
    assets_dir = static_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="static-assets")

    # SPA catch-all: serve index.html for any non-API, non-asset route
    index_path = str(static_dir / "index.html")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str):
        # Serve actual static files if they exist
        file_path = static_dir / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(index_path)


app = create_app()
