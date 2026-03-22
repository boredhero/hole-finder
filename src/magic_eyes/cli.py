"""CLI entry point for magic-eyes."""

import asyncio

import click


@click.group()
def main():
    """Magic Eyes — LiDAR terrain anomaly detection platform."""
    pass


@main.command()
@click.option("--region", required=True, help="Region name (e.g., western_pa)")
@click.option("--source", default=None, help="Specific source (default: auto from region)")
def discover(region: str, source: str | None):
    """Discover available LiDAR tiles for a region."""
    from magic_eyes.ingest.manager import discover_region
    from magic_eyes.utils.logging import setup_logging

    setup_logging()
    tiles = asyncio.run(discover_region(region))
    click.echo(f"Found {len(tiles)} tiles for {region}")
    for tile in tiles[:10]:
        click.echo(f"  {tile.source_id}: {tile.format} ({tile.file_size_bytes or '?'} bytes)")
    if len(tiles) > 10:
        click.echo(f"  ... and {len(tiles) - 10} more")


@main.command()
def seed():
    """Seed the database with known validation sites."""
    from scripts.seed_validation_sites import seed as do_seed

    asyncio.run(do_seed())


@main.command()
@click.option("--region", required=True)
def ingest(region: str):
    """Download LiDAR tiles for a region."""
    from magic_eyes.ingest.manager import discover_region, download_tiles, get_sources_for_region
    from magic_eyes.utils.logging import setup_logging

    setup_logging()

    async def _run():
        tiles = await discover_region(region)
        click.echo(f"Discovered {len(tiles)} tiles, downloading...")
        for source_name in get_sources_for_region(region):
            source_tiles = [t for t in tiles if source_name in t.source_id.lower() or True]
            paths = await download_tiles(source_tiles[:5], source_name)  # limit for safety
            click.echo(f"  Downloaded {len(paths)} tiles from {source_name}")

    asyncio.run(_run())


@main.command()
def regions():
    """List available regions."""
    from pathlib import Path

    region_dir = Path(__file__).parent.parent.parent / "configs" / "regions"
    for f in sorted(region_dir.glob("*.geojson")):
        click.echo(f"  {f.stem}")


if __name__ == "__main__":
    main()
