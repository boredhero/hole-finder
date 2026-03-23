# Anomalies Browser

A modular LiDAR analysis platform that automatically detects cave entrances, mine portals, sinkholes, and other terrain anomalies using a hybrid classical + machine learning approach.

**Live at:** [anomalies.martinospizza.dev](https://anomalies.martinospizza.dev)

## What It Does

Processes free, publicly available LiDAR elevation data (USGS 3DEP, PASDA, state GIS portals) to find underground features that are invisible to the naked eye but leave subtle signatures in terrain data:

- **Cave entrances** detected via Local Relief Models, point density voids, and multi-return analysis
- **Mine portals** found through fill-difference analysis and collapse pit morphometry
- **Sinkholes** identified with multi-scale TPI, sky-view factor, and curvature analysis
- **Other anomalies** via a plugin system that makes adding new detection passes trivial

## Architecture

```
LiDAR Data (COPC/LAZ)
    |
    v
[Processing Pipeline] --- PDAL ground classification --> DEM --> 12 terrain derivatives
    |
    v
[Detection Engine] --- 11 plugin passes (8 classical + 3 ML) --> candidate features
    |
    v
[Fusion] --- DBSCAN clustering + weighted confidence scoring --> final detections
    |
    v
[Web UI] --- React + deck.gl + MapLibre --> interactive map with satellite/topo/3D terrain
```

### Detection Passes

| Pass | Method | Best For |
|------|--------|----------|
| Fill-Difference | Priority-flood sink subtraction | Sinkholes (93% recall) |
| Local Relief Model | Multi-scale trend surface removal | Cave entrances (80% confirmed) |
| Curvature | Zevenbergen & Thorne profile/plan | Concavities |
| Sky-View Factor | Horizon angle sampling | Enclosed features |
| TPI | Multi-scale topographic position | Depressions |
| Point Density | Z-score void detection | Cave/mine openings |
| Multi-Return | Anomalous return patterns | Sub-surface openings |
| Morphometric Filter | Depth/area/circularity/k-param | False positive filtering |
| Random Forest | 10-feature classifier (sklearn) | Sinkhole classification |
| U-Net | 5-channel semantic segmentation | Pixel-level detection |
| YOLOv8 | Hillshade object detection | Cave/mine bounding boxes |

### Target Regions

- Western Pennsylvania (Allegheny Plateau karst, bituminous coal belt)
- Eastern Pennsylvania (Great Valley karst, anthracite coal region)
- West Virginia (Greenbrier County karst, extensive coal mining)
- Eastern Ohio (coal mine regions, Lockport Formation karst)
- Upstate New York (Niagara Escarpment, Lockport dolomite)

## Tech Stack

**Backend:** Python 3.12, FastAPI, SQLAlchemy + GeoAlchemy2, PostGIS, Celery + Redis, PDAL

**Frontend:** React + TypeScript, deck.gl, MapLibre GL JS, Zustand, TanStack Query, Tailwind CSS

**ML:** scikit-learn (Random Forest), PyTorch + ROCm (U-Net, YOLOv8)

**Infrastructure:** Docker, GitHub Actions CI/CD, nginx reverse proxy

## Quick Start

```bash
# Clone
git clone https://github.com/boredhero/anomalies-browser.git
cd anomalies-browser

# Backend
uv sync --extra dev
uv run pytest tests/unit/ -v     # 126 tests

# Frontend
cd frontend && pnpm install && pnpm dev
```

## Data Sources

All data sources are free and require no API keys:

- **USGS 3DEP** via Planetary Computer STAC API (COPC from `s3://usgs-lidar-public/`)
- **PASDA** (Pennsylvania Spatial Data Access)
- **WV/NY/OH** state GIS portals

## Validation

23 known cave, mine, and sinkhole sites with GPS coordinates across PA, WV, OH, and NY used as ground truth. Bulk validation against 111,000+ PASDA karst features and 11,249 PA abandoned mines.

## License

[GNU General Public License v3.0](LICENSE)
