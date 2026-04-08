"""Unified ground truth loader — orchestrates all sub-loaders and deduplicates."""

import time

from sqlalchemy.ext.asyncio import AsyncSession

from hole_finder.ingest.ground_truth.ca_blm_aml import load_ca_blm_aml
from hole_finder.ingest.ground_truth.la_subsidence import load_la_subsidence
from hole_finder.ingest.ground_truth.ma_mines import load_ma_mines
from hole_finder.ingest.ground_truth.md_karst import load_md_karst
from hole_finder.ingest.ground_truth.nc_caves import load_nc_caves
from hole_finder.ingest.ground_truth.ohio_karst import load_ohio_karst
from hole_finder.ingest.ground_truth.pa_aml import load_pa_aml
from hole_finder.ingest.ground_truth.pasda_karst import load_pasda_karst
from hole_finder.ingest.ground_truth.usgs_national import load_usgs_national
from hole_finder.ingest.ground_truth.usgs_ny_karst import load_usgs_ny_karst
from hole_finder.utils.log_manager import log


async def load_all_ground_truth(session: AsyncSession, data_dir: str) -> dict[str, int]:
    """Load all ground truth datasets. Returns counts per source."""
    t0 = time.monotonic()
    results = {}
    loaders = [
        ("pasda_karst", load_pasda_karst),
        ("pa_aml", load_pa_aml),
        ("usgs_ny", load_usgs_ny_karst),
        ("usgs_national", load_usgs_national),
        ("ohio_epa", load_ohio_karst),
        ("nc_cave_survey", load_nc_caves),
        ("md_karst_survey", load_md_karst),
        ("ma_usgs_mines", load_ma_mines),
        ("la_subsidence", load_la_subsidence),
        ("ca_blm_aml", load_ca_blm_aml),
    ]
    log.info("ground_truth_load_all_start", data_dir=data_dir, loader_count=len(loaders))
    for name, loader_fn in loaders:
        try:
            t_loader = time.monotonic()
            count = await loader_fn(session, data_dir)
            elapsed_loader = round(time.monotonic() - t_loader, 3)
            results[name] = count
            log.info("ground_truth_loaded", source=name, count=count, elapsed_s=elapsed_loader)
        except Exception as e:
            log.error("ground_truth_error", source=name, error=str(e), exception=True)
            results[name] = 0
    total = sum(results.values())
    failed = [k for k, v in results.items() if v == 0]
    elapsed = round(time.monotonic() - t0, 3)
    log.info("ground_truth_load_all_done", total_records=total, sources_loaded=len(results), sources_failed=len(failed), failed_sources=failed, per_source=results, elapsed_s=elapsed)
    return results
