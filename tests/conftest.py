"""Shared test fixtures."""

import json
from pathlib import Path

import pytest

from magic_eyes.detection.registry import PassRegistry


@pytest.fixture(autouse=True)
def ensure_passes_registered():
    """Ensure all passes are registered before each test."""
    import importlib
    import magic_eyes.detection.passes as passes_mod
    import magic_eyes.detection.passes.fill_difference as fd_mod

    # Force re-registration if registry was cleared
    importlib.reload(fd_mod)
    importlib.reload(passes_mod)
    yield


@pytest.fixture
def known_sites():
    """Load known validation sites from JSON fixture."""
    path = Path(__file__).parent / "fixtures" / "known_sites.json"
    with open(path) as f:
        data = json.load(f)
    return data["validation_sites"]
