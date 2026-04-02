"""CLI entry point for hole-finder."""

import asyncio

import click


@click.group()
def main():
    """Hole Finder — LiDAR terrain anomaly detection platform."""
    pass


@main.command()
@click.option("--lat", required=True, type=float, help="Center latitude")
@click.option("--lon", required=True, type=float, help="Center longitude")
@click.option("--radius", default=5.0, type=float, help="Radius in km (default 5)")
def discover(lat: float, lon: float, radius: float):
    """Discover available LiDAR tiles for a location."""
    from shapely.geometry import box
    from hole_finder.ingest.manager import discover_tiles_for_bbox
    from hole_finder.utils.logging import setup_logging
    setup_logging()
    r = radius / 111.32
    bbox = box(lon - r, lat - r, lon + r, lat + r)
    tiles, source = asyncio.run(discover_tiles_for_bbox(bbox, lat, lon))
    click.echo(f"Found {len(tiles)} tiles via {source}")
    for tile in tiles[:10]:
        click.echo(f"  {tile.source_id}: {tile.format} ({tile.file_size_bytes or '?'} bytes)")
    if len(tiles) > 10:
        click.echo(f"  ... and {len(tiles) - 10} more")


@main.command()
def seed():
    """Seed the database with known validation sites."""
    from scripts.seed_validation_sites import seed as do_seed
    asyncio.run(do_seed())


if __name__ == "__main__":
    main()
