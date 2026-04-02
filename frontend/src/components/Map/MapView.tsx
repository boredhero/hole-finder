import { useCallback, useEffect, useRef } from 'react';
import Map, { NavigationControl, ScaleControl, GeolocateControl, useMap } from 'react-map-gl/maplibre';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { HeatmapLayer } from '@deck.gl/aggregation-layers';
import { useControl } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';

import { useStore } from '../../store';
import { useDetections } from '../../hooks/useDetections';
import { getDetectionDetail, getTileCoverage } from '../../api/client';
import DrawControl from './DrawControl';
import type { Basemap, Detection } from '../../types';
import { FEATURE_COLORS } from '../../types';

const TERRAIN_SOURCE = {
  type: 'raster-dem' as const,
  tiles: ['/api/raster/terrain/{z}/{x}/{y}.png'],
  tileSize: 256,
  encoding: 'terrarium' as const,
  minzoom: 7,
  maxzoom: 15,
};

const MAPLIBRE_GLYPHS = 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf';

const SATELLITE_STYLE = {
  version: 8 as const,
  glyphs: MAPLIBRE_GLYPHS,
  sources: {
    'esri-satellite': {
      type: 'raster' as const,
      tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
      tileSize: 256,
      maxzoom: 18,
      attribution: 'Esri, Maxar, Earthstar Geographics',
    },
    'carto-labels': {
      type: 'raster' as const,
      tiles: ['https://basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}@2x.png'],
      tileSize: 256,
      maxzoom: 18,
      attribution: 'CARTO',
    },
    'terrain-source': TERRAIN_SOURCE,
  },
  layers: [
    {
      id: 'satellite',
      type: 'raster' as const,
      source: 'esri-satellite',
    },
    {
      id: 'labels',
      type: 'raster' as const,
      source: 'carto-labels',
    },
  ],
};

const LIDAR_STYLE = {
  version: 8 as const,
  glyphs: MAPLIBRE_GLYPHS,
  sources: {
    'topo-contours': {
      type: 'raster' as const,
      tiles: ['https://tile.opentopomap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      maxzoom: 17,
      attribution: 'OpenTopoMap',
    },
    'lidar-hillshade': {
      type: 'raster' as const,
      tiles: ['/api/raster/hillshade/{z}/{x}/{y}.png'],
      tileSize: 256,
      minzoom: 10,
      maxzoom: 16,
    },
    'terrain-source': TERRAIN_SOURCE,
  },
  layers: [
    {
      id: 'lidar-hillshade',
      type: 'raster' as const,
      source: 'lidar-hillshade',
    },
    {
      id: 'topo-base',
      type: 'raster' as const,
      source: 'topo-contours',
      paint: { 'raster-opacity': 0.55 },
    },
  ],
};

const TOPO_STYLE = {
  version: 8 as const,
  glyphs: MAPLIBRE_GLYPHS,
  sources: {
    'opentopomap': {
      type: 'raster' as const,
      tiles: ['https://tile.opentopomap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      maxzoom: 17,
      attribution: 'OpenTopoMap',
    },
    'terrain-source': TERRAIN_SOURCE,
  },
  layers: [
    {
      id: 'topo-base',
      type: 'raster' as const,
      source: 'opentopomap',
    },
  ],
};

const BASEMAP_STYLES: Record<Basemap, string | object> = {
  satellite: SATELLITE_STYLE,
  lidar: LIDAR_STYLE,
  topo: TOPO_STYLE,
  dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
};

function DeckGLOverlay(props: { layers: any[] }) {
  const overlay = useControl(() => new MapboxOverlay({ interleaved: false }));
  useEffect(() => {
    try {
      overlay.setProps({ layers: props.layers });
    } catch {
      // Ignore during style transitions when GL context is temporarily invalid
    }
  }, [overlay, props.layers]);
  return null;
}

/** Manages 3D terrain via direct map.setTerrain() call with try-catch.
 *  Using the <Map terrain={...}> prop causes react-map-gl to call setTerrain
 *  during its React render cycle, which races with MapLibre's render loop
 *  and throws uncatchable DOMExceptions. Calling it directly lets us catch. */
function TerrainController() {
  const { current: mapRef } = useMap();
  const show3DTerrain = useStore((s) => s.show3DTerrain);
  const terrainReady = useStore((s) => s.terrainReady);
  const terrainExaggeration = useStore((s) => s.terrainExaggeration);
  useEffect(() => {
    const map = mapRef?.getMap();
    if (!map) return;
    requestAnimationFrame(() => {
      try {
        if (!map.isStyleLoaded() || !map.getSource('terrain-source')) return;
        if (show3DTerrain && terrainReady) {
          map.setTerrain({ source: 'terrain-source', exaggeration: terrainExaggeration });
        } else {
          map.setTerrain(null);
        }
      } catch { /* suppress DOMException during style transitions */ }
    });
  }, [mapRef, show3DTerrain, terrainReady, terrainExaggeration]);
  return null;
}

function FlyToHandler() {
  const { current: mapRef } = useMap();
  const targetViewState = useStore((s) => s.targetViewState);
  const clearTargetViewState = useStore((s) => s.clearTargetViewState);

  useEffect(() => {
    if (targetViewState && mapRef) {
      console.log('[HoleFinder] FlyTo:', targetViewState.latitude.toFixed(4), targetViewState.longitude.toFixed(4));
      mapRef.flyTo({
        center: [targetViewState.longitude, targetViewState.latitude],
        zoom: targetViewState.zoom,
        pitch: targetViewState.pitch ?? 45,
        bearing: targetViewState.bearing ?? -15,
        duration: 2000,
      });
      clearTargetViewState();
    }
  }, [targetViewState, mapRef, clearTargetViewState]);

  return null;
}

/** Adds MVT vector tile layers for detections + ground truth. Re-adds after basemap change. */
function MVTLayerManager() {
  const { current: mapRef } = useMap();
  const showGroundTruth = useStore((s) => s.showGroundTruth);
  const setSelectedDetection = useStore((s) => s.setSelectedDetection);
  const setDrawerState = useStore((s) => s.setDrawerState);
  const setSidebarOpen = useStore((s) => s.setSidebarOpen);

  const addMVTLayers = useCallback((map: any) => {
    console.log('[MVT] addMVTLayers called, style loaded:', map.isStyleLoaded(), 'existing sources:', Object.keys(map.getStyle()?.sources || {}));
    // Detection tiles
    if (!map.getSource('detections-mvt')) {
      map.addSource('detections-mvt', {
        type: 'vector',
        tiles: [`${window.location.origin}/api/tiles/{z}/{x}/{y}.mvt?min_confidence=0.3`],
        minzoom: 6,
        maxzoom: 16,
      });
      console.log('[MVT] Added detections-mvt source');
    }
    if (!map.getLayer('detections-circles')) {
      map.addLayer({
        id: 'detections-circles',
        type: 'circle',
        source: 'detections-mvt',
        'source-layer': 'detections',
        // Zoom-dependent confidence filter: at low zoom only show high-confidence,
        // progressively reveal lower-confidence detections as user zooms in
        filter: ['>=', ['get', 'confidence'],
          ['step', ['zoom'],
            0.7,      // zoom < 12: only high confidence
            12, 0.6,  // zoom 12–14: medium-high
            14, 0.5,  // zoom 14–16: medium
            16, 0.3,  // zoom 16+: show all
          ],
        ],
        paint: {
          // Radius scales with both zoom AND confidence
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            8, ['interpolate', ['linear'], ['get', 'confidence'], 0.3, 2, 0.7, 4, 1.0, 6],
            12, ['interpolate', ['linear'], ['get', 'confidence'], 0.3, 4, 0.7, 8, 1.0, 12],
            16, ['interpolate', ['linear'], ['get', 'confidence'], 0.3, 8, 0.7, 14, 1.0, 20],
          ],
          'circle-color': ['match', ['get', 'feature_type'],
            'cave_entrance', FEATURE_COLORS.cave_entrance,
            'mine_portal', FEATURE_COLORS.mine_portal,
            'sinkhole', FEATURE_COLORS.sinkhole,
            'depression', FEATURE_COLORS.depression,
            'collapse_pit', FEATURE_COLORS.collapse_pit,
            'spring', FEATURE_COLORS.spring,
            'lava_tube', FEATURE_COLORS.lava_tube,
            'salt_dome_collapse', FEATURE_COLORS.salt_dome_collapse,
            FEATURE_COLORS.unknown,
          ],
          'circle-stroke-width': ['interpolate', ['linear'], ['zoom'], 8, 1, 14, 2, 18, 3],
          'circle-stroke-color': 'rgba(255,255,255,0.7)',
          'circle-opacity': [
            'interpolate', ['linear'], ['get', 'confidence'],
            0.3, 0.35,
            0.5, 0.55,
            0.7, 0.75,
            0.9, 0.95,
          ],
        },
      });
    }

    // Detection outline polygons (from same MVT source, 'outlines' layer)
    if (!map.getLayer('detection-outlines-fill')) {
      map.addLayer({
        id: 'detection-outlines-fill',
        type: 'fill',
        source: 'detections-mvt',
        'source-layer': 'outlines',
        paint: {
          'fill-color': '#ff1a4a',
          'fill-opacity': ['interpolate', ['linear'], ['zoom'], 10, 0.15, 13, 0.25, 15, 0.35, 18, 0.5],
        },
      });
    }
    if (!map.getLayer('detection-outlines-stroke')) {
      map.addLayer({
        id: 'detection-outlines-stroke',
        type: 'line',
        source: 'detections-mvt',
        'source-layer': 'outlines',
        paint: {
          'line-color': '#ff0044',
          'line-width': ['interpolate', ['linear'], ['zoom'], 10, 2, 13, 3, 15, 4, 17, 6, 18, 8],
          'line-opacity': 1.0,
        },
      });
    }

    console.log('[MVT] Layers added. Sources:', Object.keys(map.getStyle()?.sources || {}), 'Layers:', map.getStyle()?.layers?.map((l: any) => l.id).filter((id: string) => id.includes('detection') || id.includes('outline') || id.includes('ground')));
    // Log when MVT tiles actually load data
    map.on('sourcedata', (e: any) => {
      if (e.sourceId === 'detections-mvt' && e.isSourceLoaded) {
        const features = map.querySourceFeatures('detections-mvt', { sourceLayer: 'detections' });
        const outlines = map.querySourceFeatures('detections-mvt', { sourceLayer: 'outlines' });
        console.log('[MVT] detections-mvt loaded:', features.length, 'detection features,', outlines.length, 'outline features');
      }
    });
    // Ground truth tiles
    if (!map.getSource('ground-truth-mvt')) {
      map.addSource('ground-truth-mvt', {
        type: 'vector',
        tiles: [`${window.location.origin}/api/tiles/ground-truth/{z}/{x}/{y}.mvt`],
        minzoom: 6,
        maxzoom: 16,
      });
    }
    if (!map.getLayer('ground-truth-circles')) {
      map.addLayer({
        id: 'ground-truth-circles',
        type: 'circle',
        source: 'ground-truth-mvt',
        'source-layer': 'ground_truth',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 8, 5, 14, 10, 18, 16],
          'circle-color': '#ffd700',
          'circle-stroke-width': 2,
          'circle-stroke-color': '#ffffff',
          'circle-opacity': 0.9,
        },
      });
    }
  }, []);

  useEffect(() => {
    const map = mapRef?.getMap();
    if (!map) return;

    const setup = () => {
      addMVTLayers(map);
      console.log('[MVT] setup() complete');

      // Click detection dot → fetch detail → show in drawer/sidebar
      map.on('click', 'detections-circles', async (e: any) => {
        const feature = e.features?.[0];
        if (!feature?.properties?.id) return;
        try {
          const detail = await getDetectionDetail(feature.properties.id);
          const d: Detection = {
            id: detail.id,
            lat: e.lngLat.lat,
            lon: e.lngLat.lng,
            feature_type: detail.feature_type,
            confidence: detail.confidence,
            depth_m: detail.depth_m,
            area_m2: detail.area_m2,
            circularity: detail.circularity,
            wall_slope_deg: detail.wall_slope_deg,
            source_passes: detail.source_passes,
            morphometrics: detail.morphometrics,
            validated: detail.validated,
            validation_notes: detail.validation_notes,
          };
          setSelectedDetection(d);
          setDrawerState('detail');
          setSidebarOpen(true);
        } catch {
          // ignore fetch errors on click
        }
      });

      // Click outline polygon → same detail flow
      map.on('click', 'detection-outlines-fill', async (e: any) => {
        const feature = e.features?.[0];
        if (!feature?.properties?.id) return;
        try {
          const detail = await getDetectionDetail(feature.properties.id);
          const d: Detection = {
            id: detail.id,
            lat: e.lngLat.lat,
            lon: e.lngLat.lng,
            feature_type: detail.feature_type,
            confidence: detail.confidence,
            depth_m: detail.depth_m,
            area_m2: detail.area_m2,
            circularity: detail.circularity,
            wall_slope_deg: detail.wall_slope_deg,
            source_passes: detail.source_passes,
            morphometrics: detail.morphometrics,
            validated: detail.validated,
            validation_notes: detail.validation_notes,
          };
          setSelectedDetection(d);
          setDrawerState('detail');
          setSidebarOpen(true);
        } catch {
          // ignore fetch errors on click
        }
      });

      // Cursor
      map.on('mouseenter', 'detections-circles', () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', 'detections-circles', () => { map.getCanvas().style.cursor = ''; });
      map.on('mouseenter', 'detection-outlines-fill', () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', 'detection-outlines-fill', () => { map.getCanvas().style.cursor = ''; });
    };

    // Try immediately if style already loaded, otherwise wait for ready/style.load
    if (map.isStyleLoaded()) {
      setup();
    } else {
      map.once('style.load', setup);
    }
    // Also listen for our custom ready event (fired from Map onLoad)
    map.on('holefinder:ready', () => {
      if (!map.getSource('detections-mvt')) setup();
    });

    // Re-add layers after basemap change destroys them
    map.on('style.load', () => {
      // For external styles (topo/dark), terrain source isn't baked in — add it
      if (!map.getSource('terrain-source')) {
        try {
          map.addSource('terrain-source', TERRAIN_SOURCE);
        } catch { /* source may already exist during rapid style switches */ }
      }
      addMVTLayers(map);
    });
  }, [mapRef, addMVTLayers, setSelectedDetection, setDrawerState, setSidebarOpen]);

  // Toggle ground truth visibility
  useEffect(() => {
    const map = mapRef?.getMap();
    if (!map || !map.getLayer('ground-truth-circles')) return;
    map.setLayoutProperty('ground-truth-circles', 'visibility', showGroundTruth ? 'visible' : 'none');
  }, [showGroundTruth, mapRef]);

  return null;
}

/** Fetches tile coverage GeoJSON and renders fill+line layers showing LiDAR vs AWS source. */
function TileCoverageLayer() {
  const { current: mapRef } = useMap();
  const showTileCoverage = useStore((s) => s.showTileCoverage);
  const bbox = useStore((s) => s.bbox);
  const viewState = useStore((s) => s.viewState);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  // Idempotent: ensures source+layers exist on the current style. Returns true if ready.
  const ensureLayers = useCallback((map: any): boolean => {
    try {
      if (!map.isStyleLoaded()) return false;
      if (!map.getSource('tile-coverage')) {
        map.addSource('tile-coverage', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
      }
      if (!map.getLayer('tile-coverage-fill')) {
        map.addLayer({
          id: 'tile-coverage-fill', type: 'fill', source: 'tile-coverage',
          paint: {
            'fill-color': ['match', ['get', 'source'], 'lidar', '#eab308', '#06b6d4'],
            'fill-opacity': ['match', ['get', 'source'], 'lidar', 0.25, 0.12],
          },
        });
      }
      if (!map.getLayer('tile-coverage-line')) {
        map.addLayer({
          id: 'tile-coverage-line', type: 'line', source: 'tile-coverage',
          paint: {
            'line-color': ['match', ['get', 'source'], 'lidar', '#eab308', '#06b6d4'],
            'line-width': ['match', ['get', 'source'], 'lidar', 2.5, 1],
            'line-opacity': ['match', ['get', 'source'], 'lidar', 0.9, 0.5],
          },
        });
      }
      if (!map.getLayer('tile-coverage-label')) {
        map.addLayer({
          id: 'tile-coverage-label', type: 'symbol', source: 'tile-coverage',
          filter: ['==', ['get', 'source'], 'lidar'],
          layout: { 'text-field': 'LiDAR', 'text-size': 11, 'text-font': ['Open Sans Semibold'], 'text-allow-overlap': false },
          paint: { 'text-color': '#eab308', 'text-halo-color': 'rgba(0,0,0,0.8)', 'text-halo-width': 1.5 },
        });
      }
      const vis = useStore.getState().showTileCoverage ? 'visible' : 'none';
      for (const id of ['tile-coverage-fill', 'tile-coverage-line', 'tile-coverage-label']) {
        if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
      }
      return true;
    } catch {
      return false;
    }
  }, []);
  // Register for style.load + holefinder:ready so layers survive basemap switches
  useEffect(() => {
    const map = mapRef?.getMap();
    if (!map) return;
    const handler = () => ensureLayers(map);
    if (map.isStyleLoaded()) handler();
    map.on('style.load', handler);
    map.on('holefinder:ready', handler);
    return () => { map.off('style.load', handler); map.off('holefinder:ready', handler); };
  }, [mapRef, ensureLayers]);
  // Toggle visibility
  useEffect(() => {
    const map = mapRef?.getMap();
    if (!map) return;
    const vis = showTileCoverage ? 'visible' : 'none';
    for (const id of ['tile-coverage-fill', 'tile-coverage-line', 'tile-coverage-label']) {
      try { if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis); } catch { /* style transition */ }
    }
  }, [showTileCoverage, mapRef]);
  // Fetch coverage data on viewport change (debounced 500ms)
  useEffect(() => {
    if (!showTileCoverage || !bbox || !viewState) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      if (abortRef.current) abortRef.current.abort();
      abortRef.current = new AbortController();
      const z = Math.min(Math.floor(viewState.zoom), 15);
      if (z < 8) return;
      try {
        const geojson = await getTileCoverage(bbox[0], bbox[1], bbox[2], bbox[3], z);
        const map = mapRef?.getMap();
        if (!map) return;
        ensureLayers(map);
        if (map.getSource('tile-coverage')) {
          (map.getSource('tile-coverage') as any).setData(geojson);
        }
      } catch { /* fetch aborted or failed */ }
    }, 500);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [showTileCoverage, bbox, viewState, mapRef, ensureLayers]);
  return null;
}

const DEFAULT_VIEW = { longitude: -79.71, latitude: 39.80, zoom: 13, pitch: 45, bearing: -15 };

export default function MapView() {
  const basemap = useStore((s) => s.basemap);
  const showHeatmap = useStore((s) => s.showHeatmap);
  const setBbox = useStore((s) => s.setBbox);
  const drawingAOI = useStore((s) => s.drawingAOI);
  const setDrawnAOI = useStore((s) => s.setDrawnAOI);
  const storedViewState = useStore((s) => s.viewState);
  const setViewState = useStore((s) => s.setViewState);
  const setSearchStale = useStore((s) => s.setSearchStale);

  // Heatmap still uses deck.gl + useDetections (playground only)
  const { data: detections = [] } = useDetections();

  const heatmapLayers = showHeatmap && detections.length > 0
    ? [new HeatmapLayer({
        id: 'heatmap',
        data: detections,
        getPosition: (d: Detection) => [d.lon, d.lat],
        getWeight: (d: Detection) => d.confidence,
        radiusPixels: 40,
        intensity: 1,
        threshold: 0.1,
        opacity: 0.6,
      })]
    : [];

  const handleMoveEnd = useCallback((evt: any) => {
    const map = evt.target;
    const bounds = map.getBounds();
    setBbox([bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()]);
    const center = map.getCenter();
    setViewState({
      longitude: center.lng,
      latitude: center.lat,
      zoom: map.getZoom(),
      pitch: map.getPitch(),
      bearing: map.getBearing(),
    });
    setSearchStale(true);
  }, [setBbox, setViewState, setSearchStale]);

  return (
    <Map
      initialViewState={storedViewState ?? DEFAULT_VIEW}
      style={{ width: '100%', height: '100%' }}
      mapStyle={BASEMAP_STYLES[basemap] as any}
      onMoveEnd={handleMoveEnd}
      onLoad={(evt) => {
        const map = evt.target;
        const bounds = map.getBounds();
        setBbox([bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()]);
        // Suppress terrain tile decode errors — non-fatal, map shows flat terrain for failed tiles
        map.on('error', (e: any) => {
          if (e?.error?.message?.includes('usable') || e?.sourceId === 'terrain-source') return;
          console.warn('[MapView] Map error:', e?.error?.message || e);
        });
        // Dispatch custom event so MVTLayerManager knows map is ready
        map.fire('holefinder:ready');
      }}
    >
      {heatmapLayers.length > 0 && <DeckGLOverlay layers={heatmapLayers} />}
      <TerrainController />
      <MVTLayerManager />
      <TileCoverageLayer />
      <FlyToHandler />
      <DrawControl
        active={drawingAOI}
        onDrawCreate={(geom) => setDrawnAOI(geom)}
        onDrawDelete={() => setDrawnAOI(null)}
      />
      <NavigationControl position="bottom-right" />
      <ScaleControl position="bottom-left" />
      <GeolocateControl position="bottom-right" />
    </Map>
  );
}
