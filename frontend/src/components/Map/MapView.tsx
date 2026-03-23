import { useCallback, useMemo } from 'react';
import Map, { NavigationControl, ScaleControl, GeolocateControl } from 'react-map-gl/maplibre';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { ScatterplotLayer } from '@deck.gl/layers';
import { HeatmapLayer } from '@deck.gl/aggregation-layers';
import { useControl } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';

import { useStore } from '../../store';
import { useDetections, useGroundTruth } from '../../hooks/useDetections';
import DrawControl from './DrawControl';
import type { Basemap, Detection, GroundTruthSite } from '../../types';
import { FEATURE_COLORS } from '../../types';

const SATELLITE_STYLE = {
  version: 8 as const,
  sources: {
    'esri-satellite': {
      type: 'raster' as const,
      tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
      tileSize: 256,
      maxzoom: 18,
      attribution: 'Esri, Maxar, Earthstar Geographics',
    },
  },
  layers: [{
    id: 'satellite',
    type: 'raster' as const,
    source: 'esri-satellite',
  }],
};

const BASEMAP_STYLES: Record<Basemap, string | object> = {
  satellite: SATELLITE_STYLE,
  topo: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
};

function hexToRgb(hex: string): [number, number, number] {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return [r, g, b];
}

function DeckGLOverlay(props: { layers: any[] }) {
  const overlay = useControl(() => new MapboxOverlay({ interleaved: false }));
  overlay.setProps({ layers: props.layers });
  return null;
}

export default function MapView() {
  const basemap = useStore((s) => s.basemap);
  const showHeatmap = useStore((s) => s.showHeatmap);
  const showGroundTruth = useStore((s) => s.showGroundTruth);
  const show3DTerrain = useStore((s) => s.show3DTerrain);
  const terrainExaggeration = useStore((s) => s.terrainExaggeration);
  const setBbox = useStore((s) => s.setBbox);
  const setSelectedDetection = useStore((s) => s.setSelectedDetection);
  const setHoveredDetectionId = useStore((s) => s.setHoveredDetectionId);
  const hoveredId = useStore((s) => s.hoveredDetectionId);
  const setSidebarOpen = useStore((s) => s.setSidebarOpen);
  const drawingAOI = useStore((s) => s.drawingAOI);
  const setDrawnAOI = useStore((s) => s.setDrawnAOI);

  const { data: detections = [] } = useDetections();
  const { data: groundTruth = [] } = useGroundTruth();

  const handleMoveEnd = useCallback((evt: any) => {
    const bounds = evt.target.getBounds();
    setBbox([bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()]);
  }, [setBbox]);

  const layers = useMemo(() => {
    const result: any[] = [];

    // Heatmap layer
    if (showHeatmap && detections.length > 0) {
      result.push(new HeatmapLayer({
        id: 'heatmap',
        data: detections,
        getPosition: (d: Detection) => [d.lon, d.lat],
        getWeight: (d: Detection) => d.confidence,
        radiusPixels: 40,
        intensity: 1,
        threshold: 0.1,
        opacity: 0.6,
      }));
    }

    // Detection scatter layer
    if (detections.length > 0) {
      result.push(new ScatterplotLayer({
        id: 'detections',
        data: detections,
        getPosition: (d: Detection) => [d.lon, d.lat],
        getRadius: (d: Detection) => 4 + d.confidence * 12,
        getFillColor: (d: Detection) => {
          const color = hexToRgb(FEATURE_COLORS[d.feature_type] || '#6b7280');
          const alpha = d.id === hoveredId ? 255 : 180;
          return [...color, alpha];
        },
        getLineColor: [255, 255, 255, 120],
        lineWidthMinPixels: 1,
        radiusMinPixels: 4,
        radiusMaxPixels: 20,
        pickable: true,
        onClick: ({ object }: { object?: Detection }) => {
          if (object) {
            setSelectedDetection(object);
            setSidebarOpen(true);
          }
        },
        onHover: ({ object }: { object?: Detection }) => {
          setHoveredDetectionId(object?.id ?? null);
        },
        updateTriggers: {
          getFillColor: [hoveredId],
        },
      }));
    }

    // Ground truth markers
    if (showGroundTruth && groundTruth.length > 0) {
      result.push(new ScatterplotLayer({
        id: 'ground-truth',
        data: groundTruth,
        getPosition: (d: GroundTruthSite) => [d.lon, d.lat],
        getRadius: 8,
        getFillColor: [255, 215, 0, 200],
        getLineColor: [255, 255, 255, 255],
        lineWidthMinPixels: 2,
        radiusMinPixels: 6,
        radiusMaxPixels: 12,
        pickable: true,
        onClick: ({ object }: { object?: GroundTruthSite }) => {
          if (object) {
            // Show in sidebar
          }
        },
      }));
    }

    return result;
  }, [detections, groundTruth, showHeatmap, showGroundTruth, hoveredId, setSelectedDetection, setHoveredDetectionId, setSidebarOpen]);

  return (
    <Map
      initialViewState={{
        longitude: -79.0,
        latitude: 40.5,
        zoom: 7,
      }}
      style={{ width: '100%', height: '100%' }}
      mapStyle={BASEMAP_STYLES[basemap] as any}
      onMoveEnd={handleMoveEnd}
      onLoad={(evt) => {
        const map = evt.target;
        const bounds = map.getBounds();
        setBbox([bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()]);

        // Add terrain source for 3D
        if (!map.getSource('terrain-source')) {
          map.addSource('terrain-source', {
            type: 'raster-dem',
            tiles: ['https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png'],
            tileSize: 256,
            encoding: 'terrarium',
            maxzoom: 15,
          });
        }
      }}
      terrain={show3DTerrain ? { source: 'terrain-source', exaggeration: terrainExaggeration } : undefined}
      cursor={hoveredId ? 'pointer' : 'grab'}
    >
      <DeckGLOverlay layers={layers} />
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
