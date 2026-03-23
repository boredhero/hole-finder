"""Debug endpoints — performance profiling.

Gated behind HOLE_FINDER_DEBUG=1 env var. Off by default in production
to avoid leaking system info that could help attackers fingerprint the server.
"""

import os
import platform
import shutil
from typing import Any

from fastapi import APIRouter, HTTPException

from hole_finder.utils.perf import get_cpu_count, get_gpu_info, get_memory_mb

router = APIRouter(prefix="/api/debug", tags=["debug"])


def _check_debug_enabled() -> None:
    """Raise 404 if debug mode is not enabled — makes endpoint invisible."""
    if os.environ.get("HOLE_FINDER_DEBUG", "").strip() not in ("1", "true", "yes"):
        raise HTTPException(status_code=404, detail="Not found")


@router.get("/system-info")
async def system_info() -> dict[str, Any]:
    """Return hardware/software info for diagnosing performance issues.

    Only available when HOLE_FINDER_DEBUG=1 is set. Returns 404 otherwise
    so the endpoint doesn't even show up in scans.
    """
    _check_debug_enabled()

    gpu = get_gpu_info()

    info: dict[str, Any] = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu": {
            "count_os": os.cpu_count(),
            "count_available": get_cpu_count(),
            "arch": platform.machine(),
        },
        "memory": {
            "rss_mb": round(get_memory_mb(), 1),
        },
        "gpu": gpu,
    }

    for tool in ["pdal", "gdaldem", "whitebox_tools"]:
        info[f"has_{tool}"] = shutil.which(tool) is not None

    try:
        import cupy
        info["cupy_version"] = cupy.__version__
    except ImportError:
        info["cupy_version"] = None

    try:
        import torch
        info["torch_version"] = torch.__version__
        info["torch_cuda"] = torch.cuda.is_available()
    except ImportError:
        info["torch_version"] = None

    try:
        import scipy
        info["scipy_version"] = scipy.__version__
    except ImportError:
        info["scipy_version"] = None

    return info
