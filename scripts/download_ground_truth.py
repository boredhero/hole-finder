#!/usr/bin/env python3
"""Download all ground truth datasets for validation.

Usage: uv run python scripts/download_ground_truth.py [--data-dir /data/magic-eyes]

Downloads:
- PASDA karst features shapefile (111K+ points)
- PA AML inventory shapefile (11,249 mines)
- USGS NY karst closed depressions (5,023 features)
- USGS national karst map shapefile (269MB)
"""

import asyncio
import zipfile
from pathlib import Path

import click
import httpx

from magic_eyes.config import settings
from magic_eyes.utils.logging import log, setup_logging

DOWNLOADS = {
    "pasda_karst": {
        "url": "https://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=3073",
        "description": "PASDA Karst Features (111K+ points)",
        "notes": "Must be downloaded manually from PASDA website — no direct download link available",
    },
    "pa_aml": {
        "url": "https://newdata-padep.opendata.arcgis.com/datasets/PADEP-1::abandoned-mine-land-inventory-aml-problem-areas/about",
        "description": "PA Abandoned Mine Land Inventory (11,249 sites)",
        "notes": "Download shapefile from ArcGIS Hub",
    },
    "usgs_ny_karst": {
        "url": "https://www.sciencebase.gov/catalog/item/562a313ae4b011227bf1fe23",
        "description": "USGS NY Closed Depression Inventory (5,023 features)",
        "notes": "Download geodatabase from USGS ScienceBase",
    },
    "usgs_national": {
        "url": "https://pubs.usgs.gov/of/2014/1156/",
        "description": "USGS National Karst Map (all US)",
        "notes": "Download GIS shapefile package (269MB)",
    },
}


@click.command()
@click.option("--data-dir", default=None, help="Data directory (default: from config)")
def main(data_dir: str | None):
    """Print instructions for downloading ground truth datasets."""
    setup_logging()
    data_path = Path(data_dir) if data_dir else settings.data_dir
    gt_dir = data_path / "ground_truth"

    print("\n=== Ground Truth Dataset Download Guide ===\n")
    print(f"Target directory: {gt_dir}\n")

    for name, info in DOWNLOADS.items():
        target = gt_dir / name
        exists = target.exists() and any(target.iterdir()) if target.exists() else False
        status = "EXISTS" if exists else "NEEDED"

        print(f"[{status}] {info['description']}")
        print(f"  Dir:   {target}")
        print(f"  URL:   {info['url']}")
        print(f"  Notes: {info['notes']}")
        print()

    print("After downloading, place files in the directories listed above.")
    print("Then run: uv run python scripts/seed_validation_sites.py")
    print("And use the ground truth loaders to import into PostGIS.")


if __name__ == "__main__":
    main()
