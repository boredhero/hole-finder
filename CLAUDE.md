# Hole Finder — Complete Development Guide

## What This Is
LiDAR terrain anomaly detection platform. Downloads free public LiDAR data, processes it through native compiled tools (PDAL/GDAL/WhiteboxTools), runs 11 detection passes to find caves, mines, sinkholes, and stores results permanently in PostGIS. React frontend with deck.gl map shows everything.

**Live:** holefinder.martinospizza.dev and anomalies.martinospizza.dev
**Repo:** github.com/boredhero/anomalies-browser (GPL-3.0)
**Project dir:** ~/Desktop/hole-finder

## Architecture At a Glance

```
COPC/LAZ tile
  → PDAL (C++): ground classify → DEM
  → GDAL (C) + WhiteboxTools (Rust): 11 derivatives in parallel
  → 11 detection passes (Python, consume rasters only, parallel ThreadPool)
  → DBSCAN fusion → PostGIS (permanent)
  → React + deck.gl + MapLibre frontend
```

## Critical Rules (READ THESE)
1. **Push to `develop` only.** Owner merges to main. Never merge yourself.
2. **No Co-Authored-By lines** in commits. Ever.
3. **Top-level imports.** No inline imports inside functions unless circular dependency.
4. **Detection passes NEVER compute derivatives.** They consume pre-computed GeoTIFF rasters via `input_data.derivatives["name"]`. If a derivative isn't there, return empty.
5. **Processing uses native tools only.** GDAL/WhiteboxTools/PDAL subprocesses, not numpy. Python reads results with rasterio.
6. **Tests use the real native pipeline.** No numpy fallbacks. Tests that need GDAL/WBT are skipped if not available (CI skips them, .111 runs them).
7. **All processed data is permanent.** Tiles, derivatives, detections survive reboots/deploys. Stored on SSD at /data and in PostGIS volumes.
8. **Don't be lazy about performance.** Multi-core via ProcessPoolExecutor for derivatives, ThreadPoolExecutor for passes. No single-threaded Python loops on real data.
9. **Everything is Dockerized** with restart:unless-stopped. After reboot, everything comes back.
10. **Don't overwrite nginx config** on .69 — certbot manages SSL there.

## Infrastructure

### Compute Node (192.168.1.111)
- Ryzen 7 5800X3D (8c/16t), 64GB RAM
- RX 6900 XT (17.2GB VRAM), ROCm 7.2.0, PyTorch 2.5.1+rocm6.2
- 2TB NVMe (OS), 1TB Samsung SSD at /data (LiDAR storage, ext4, fstab)
- PDAL 2.9.3, GDAL 3.12.2, WhiteboxTools installed
- Docker 29.3.0 running 6 containers:
  - hole-finder-api (port 9747→8000, FastAPI + built frontend)
  - hole-finder-db (PostGIS 16, 127.0.0.1:5432)
  - hole-finder-redis (Redis 7, 127.0.0.1:6379)
  - hole-finder-worker (Celery: ingest/process/detect queues, 4 concurrency)
  - hole-finder-gpu-worker (Celery: gpu queue, /dev/kfd+/dev/dri, HSA_OVERRIDE_GFX_VERSION=10.3.0)
  - hole-finder-autoheal
- Git repo at ~/anomalies-browser (for running tests — deployment is via Docker image)
- Arch Linux

### Gateway Node (192.168.1.69)
- i7-6700K, 32GB RAM, R9 Fury (NO ROCm — too old, GCN3)
- nginx reverse proxy ONLY: holefinder.martinospizza.dev → 192.168.1.111:9747
- TLS via certbot, auto-renew timer active
- Security headers: HSTS, X-Frame-Options, X-Content-Type-Options, Referrer-Policy
- SSH jumpbox to .111 for CI/CD
- Arch Linux

### CI/CD (GitHub Actions)
- **Test workflow** (.github/workflows/test.yml): runs on push to develop, cancel-in-progress. uv install → pytest → frontend build. No linter in CI.
- **Deploy workflow** (.github/workflows/deploy.yml): runs on push to main. Build frontend → Docker image → push GHCR → SSH .69 → SCP to .111 → docker compose up → nginx reload → health check.
- Secrets: DEPLOY_HOST (boredhero.dyndns.org), DEPLOY_USER, DEPLOY_SSH_KEY, GHCR_TOKEN, POSTGRES_PASSWORD
- Docker image: ghcr.io/boredhero/anomalies-browser:latest

## Project Structure (key files)

```
src/hole_finder/
  main.py                          # FastAPI app factory, SPA serving
  config.py                        # Pydantic Settings from .env
  db/models.py                     # SQLAlchemy + GeoAlchemy2 ORM
  db/repositories.py               # Spatial queries (get_detections_near_point etc)
  api/routes/
    detections.py                  # GET /api/detections (bbox query), GET /api/detections/{id}
    jobs.py                        # GET/POST /api/jobs, cancel
    validation.py                  # POST validate, GET/POST ground-truth
    comments.py                    # Comments + saved detections
    tiles.py                       # Vector tiles (MVT) via ST_AsMVT
    raster_tiles.py                # Hillshade/terrain-RGB PNG tiles
    exports.py                     # GeoJSON/CSV download
    regions.py                     # List/get region GeoJSON
    websocket.py                   # WS /ws/jobs for progress
  processing/
    pipeline.py                    # ProcessingPipeline orchestrator
    derivatives.py                 # GDAL/WBT subprocess wrappers + parallel orchestrator
    tile_manager.py                # R-tree spatial index
    point_cloud.py                 # Density + multi-return analysis
  detection/
    base.py                        # DetectionPass ABC, Candidate, PassInput, FeatureType
    registry.py                    # PassRegistry singleton + @register_pass
    runner.py                      # PassRunner (parallel, TOML config, timing)
    fusion.py                      # DBSCAN + weighted confidence
    passes/                        # 11 passes (8 classical + 3 ML)
  workers/
    celery_app.py                  # 4 queues: ingest, process, detect, gpu
    tasks.py                       # download_tile, process_tile, run_detection, run_ml_pass
  ml/training.py                   # RF/UNet training data extraction
  ingest/
    sources/                       # USGS 3DEP, PASDA, WV, NY, OH, NC, MD
    ground_truth/                  # Loaders for PASDA karst (111K), PA AML (11K), USGS NY, OH, NC, MD, MA, LA, CA

frontend/src/
  components/Map/MapView.tsx       # MapLibre + deck.gl + 3D terrain
  components/Map/DrawControl.tsx   # AOI polygon drawing
  components/Sidebar/              # FilterPanel, DetailPanel (with comments), JobPanel
  store/index.ts                   # Zustand state
  hooks/                           # TanStack Query hooks
  api/client.ts                    # Typed fetch wrapper

configs/passes/                    # TOML detection configs (cave_hunting, sinkhole_survey, mine_detection, salt_dome_detection, lava_tube_detection)
configs/regions/                   # 13 GeoJSON region polygons (PA, WV, OH, NY, NC, MD, MA, LA, CA)

tests/unit/                        # 116+ tests using native GDAL/WBT pipeline
tests/validation/                  # Parametrized tests against 36 known sites
tests/fixtures/known_sites.json    # 36 validation coordinates
```

## Database Schema (PostGIS)
- **detections**: id, tile_id (nullable FK), feature_type enum, geometry POINT 4326, confidence, depth_m, area_m2, circularity, source_passes JSONB, morphometrics JSONB, validated bool
- **ground_truth_sites**: name, feature_type, geometry, source enum
- **tiles**: filename, file_path, bbox POLYGON, dem_path, status
- **datasets**: source, bbox, tile_count, status
- **jobs**: job_type, status, progress, region POLYGON, config JSONB
- **comments**: detection_id FK, text, author
- **saved_detections**: detection_id FK, label, color, notes
- **validation_events**: detection_id FK, verdict enum, notes
- **pass_results**: detection_id FK, pass_name, raw_score

All geometry columns have GIST spatial indexes.

## Detection Passes (11 total)
| Pass | Type | Consumes | Detects |
|------|------|----------|---------|
| fill_difference | Classical | fill_difference raster | Depressions (93% recall) |
| local_relief_model | Classical | lrm_50m, lrm_100m, lrm_200m | Cave entrances (gold standard) |
| curvature | Classical | profile_curvature | Concavities |
| sky_view_factor | Classical | svf | Enclosed features |
| tpi | Classical | tpi | Depressions (multi-scale) |
| point_density | Classical | raw point cloud | Voids (cave/mine openings) |
| multi_return | Classical | raw point cloud | Sub-surface openings |
| morphometric_filter | Classical | fill_difference, slope | Full morphometrics + classification |
| random_forest | ML (CPU) | fill_difference, slope, tpi, svf | Sinkhole classification |
| unet_segmentation | ML (GPU) | hillshade, slope, curvature, tpi, svf | Pixel-level segmentation |
| yolo_detector | ML (GPU) | hillshade | Object detection on hillshade |

ML passes return empty if no trained model exists. They degrade gracefully.

## How To: Common Tasks

### Run tests
```bash
# On .111 (fast, has GDAL/WBT):
ssh noah@192.168.1.111 'cd ~/anomalies-browser && git pull origin develop && uv run pytest tests/unit/ -v'

# Locally (skips native tests if no GDAL):
uv run pytest tests/unit/ -v
```

### Process a real tile
```bash
# Inside the Docker container on .111:
docker exec hole-finder-worker uv run python3 -c "
from hole_finder.processing.pipeline import ProcessingPipeline
from pathlib import Path
result = ProcessingPipeline(output_dir=Path('/data/hole-finder/processed/my_tile')).process_dem_file(Path('/data/hole-finder/processed/my_tile/dem.tif'), force=True)
print(result.derivative_paths)
"
```

### Store detections in PostGIS
After detection, transform UTM→WGS84 with pyproj, create Detection ORM objects with from_shape(Point(lon,lat)), add to session, commit.

### Add a new detection pass
1. Create `src/hole_finder/detection/passes/my_pass.py`
2. Implement DetectionPass ABC (name, version, required_derivatives, run)
3. Decorate with `@register_pass`
4. Add import to `passes/__init__.py`
5. Add to TOML configs in `configs/passes/`
6. Write test in `tests/unit/test_detection_passes.py`

### Add a new API endpoint
1. Create or edit file in `src/hole_finder/api/routes/`
2. Register router in `src/hole_finder/main.py`
3. Add test in `tests/unit/test_api.py` route structure test
4. If new DB table needed: add model in `db/models.py`, write migration in `alembic/versions/`

## Current State
- 798 real detections from Laurel Caverns tile stored in PostGIS
- 36 validation sites seeded (PA, WV, OH, NY, NC, MD, MA, LA, CA caves + mines + sinkholes)
- 116+ tests passing on .111
- Both domains live with TLS
- 1 COPC tile downloaded and processed (28.2M points → 1500x1500 DEM → 11 derivatives → detections)
- Derivatives compute in 1.6s (parallel native), detection in ~66s (7 passes)

## What's NOT Done Yet
- Only 1 tile processed — need to process more regions
- ML models not trained (RF, UNet, YOLO) — infrastructure ready, need training data
- No real-time tile processing from UI job submission (Celery tasks wired but not end-to-end tested)
- Performance: detection loop (66s) could be faster with vectorized morphometrics
- Frontend needs polish: some features wired but not battle-tested
