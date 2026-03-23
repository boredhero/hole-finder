"""Unit tests for ingest module — source discovery, region loading, manager."""

import json
from pathlib import Path

import pytest

from hole_finder.ingest.manager import (
    SOURCE_REGISTRY,
    get_source,
    get_sources_for_region,
    load_region_bbox,
)
from hole_finder.ingest.sources.usgs_3dep import USGS3DEPSource


class TestSourceRegistry:
    def test_all_sources_registered(self):
        assert "usgs_3dep" in SOURCE_REGISTRY
        assert "pasda" in SOURCE_REGISTRY
        assert "wv" in SOURCE_REGISTRY
        assert "ny" in SOURCE_REGISTRY
        assert "oh" in SOURCE_REGISTRY
        assert "nc" in SOURCE_REGISTRY
        assert "md" in SOURCE_REGISTRY

    def test_get_source(self):
        source = get_source("usgs_3dep")
        assert source.name == "usgs_3dep"

    def test_unknown_source_raises(self):
        with pytest.raises(KeyError, match="nonexistent"):
            get_source("nonexistent")


class TestRegionSources:
    def test_western_pa_sources(self):
        sources = get_sources_for_region("western_pa")
        assert "usgs_3dep" in sources
        assert "pasda" in sources

    def test_west_virginia_sources(self):
        sources = get_sources_for_region("west_virginia")
        assert "usgs_3dep" in sources
        assert "wv" in sources

    def test_western_nc_sources(self):
        sources = get_sources_for_region("western_nc")
        assert "usgs_3dep" in sources
        assert "nc" in sources

    def test_western_md_sources(self):
        sources = get_sources_for_region("western_md")
        assert "usgs_3dep" in sources
        assert "md" in sources

    def test_south_louisiana_sources(self):
        sources = get_sources_for_region("south_louisiana")
        assert "usgs_3dep" in sources

    def test_sierra_nevada_sources(self):
        sources = get_sources_for_region("sierra_nevada")
        assert "usgs_3dep" in sources

    def test_unknown_region_defaults_to_3dep(self):
        sources = get_sources_for_region("mars")
        assert sources == ["usgs_3dep"]


class TestRegionLoading:
    def test_load_western_pa(self):
        bbox = load_region_bbox("western_pa")
        assert bbox is not None
        assert bbox.is_valid
        # Western PA should cover roughly -80.6 to -78.5 longitude
        bounds = bbox.bounds
        assert bounds[0] < -78  # west
        assert bounds[2] > -81  # east

    def test_load_all_regions(self):
        region_names = [
            "western_pa", "eastern_pa", "west_virginia", "eastern_ohio", "upstate_ny",
            "western_nc", "western_md", "western_ma",
            "south_louisiana", "north_louisiana",
            "northern_ca_lava", "sierra_nevada", "southern_ca_desert",
        ]
        for name in region_names:
            bbox = load_region_bbox(name)
            assert bbox.is_valid, f"Invalid bbox for {name}"
            assert bbox.area > 0, f"Empty bbox for {name}"

    def test_load_nonexistent_region(self):
        with pytest.raises(FileNotFoundError):
            load_region_bbox("atlantis")


class TestUSGS3DEPSource:
    def test_source_name(self):
        source = USGS3DEPSource()
        assert source.name == "usgs_3dep"

    def test_parse_stac_item(self):
        source = USGS3DEPSource()
        item = {
            "id": "USGS_LPC_PA_SouthCentral_2018_D19_4288000e_9318000n",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-79.7, 39.8], [-79.6, 39.8], [-79.6, 39.9], [-79.7, 39.9], [-79.7, 39.8]]],
            },
            "properties": {
                "datetime": "2018-06-15T00:00:00Z",
                "proj:epsg": 6344,
            },
            "assets": {
                "data": {
                    "href": "https://example.com/tile.copc.laz",
                    "file:size": 52428800,
                },
            },
        }
        tile = source._parse_stac_item(item)
        assert tile is not None
        assert tile.format == "copc"
        assert tile.acquisition_year == 2018
        assert tile.file_size_bytes == 52428800
        assert "copc.laz" in tile.filename

    def test_parse_invalid_item_returns_none(self):
        source = USGS3DEPSource()
        tile = source._parse_stac_item({"id": "broken", "properties": {}})
        assert tile is None


class TestKnownSites:
    def test_sites_json_valid(self):
        sites_path = Path(__file__).parent.parent / "fixtures" / "known_sites.json"
        with open(sites_path) as f:
            data = json.load(f)

        sites = data["validation_sites"]
        assert len(sites) >= 35, f"Expected >=35 validation sites, got {len(sites)}"

        for site in sites:
            assert "name" in site
            assert "lat" in site
            assert "lon" in site
            assert "type" in site
            assert -90 <= site["lat"] <= 90
            assert -180 <= site["lon"] <= 180
            assert site["type"] in ("cave_entrance", "mine_portal", "sinkhole", "depression")

    def test_sites_cover_all_states(self):
        sites_path = Path(__file__).parent.parent / "fixtures" / "known_sites.json"
        with open(sites_path) as f:
            data = json.load(f)

        states = {s["state"] for s in data["validation_sites"]}
        assert "PA" in states
        assert "WV" in states
        assert "OH" in states
        assert "NY" in states
        assert "NC" in states
        assert "MD" in states
        assert "MA" in states
        assert "LA" in states
        assert "CA" in states
