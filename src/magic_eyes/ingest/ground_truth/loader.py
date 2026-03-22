"""Unified ground truth loader — orchestrates all sub-loaders and deduplicates."""

from sqlalchemy.ext.asyncio import AsyncSession

from magic_eyes.ingest.ground_truth.pasda_karst import load_pasda_karst
from magic_eyes.ingest.ground_truth.pa_aml import load_pa_aml
from magic_eyes.ingest.ground_truth.usgs_ny_karst import load_usgs_ny_karst
from magic_eyes.ingest.ground_truth.usgs_national import load_usgs_national
from magic_eyes.ingest.ground_truth.ohio_karst import load_ohio_karst
from magic_eyes.utils.logging import log


async def load_all_ground_truth(session: AsyncSession, data_dir: str) -> dict[str, int]:
    """Load all ground truth datasets. Returns counts per source."""
    results = {}

    loaders = [
        ("pasda_karst", load_pasda_karst),
        ("pa_aml", load_pa_aml),
        ("usgs_ny", load_usgs_ny_karst),
        ("usgs_national", load_usgs_national),
        ("ohio_epa", load_ohio_karst),
    ]

    for name, loader_fn in loaders:
        try:
            count = await loader_fn(session, data_dir)
            results[name] = count
            log.info("ground_truth_loaded", source=name, count=count)
        except Exception as e:
            log.error("ground_truth_error", source=name, error=str(e))
            results[name] = 0

    return results
