import { useEffect, useRef } from 'react';
import { useMap } from 'react-map-gl/maplibre';
import {
  TerraDraw,
  TerraDrawPolygonMode,
  TerraDrawSelectMode,
  TerraDrawRenderMode,
} from 'terra-draw';
import { TerraDrawMapLibreGLAdapter } from 'terra-draw-maplibre-gl-adapter';

interface DrawControlProps {
  active: boolean;
  onDrawCreate: (geometry: GeoJSON.Geometry) => void;
  onDrawDelete: () => void;
}

export default function DrawControl({ active, onDrawCreate, onDrawDelete }: DrawControlProps) {
  const { current: mapRef } = useMap();
  const drawRef = useRef<TerraDraw | null>(null);
  const onDrawCreateRef = useRef(onDrawCreate);
  onDrawCreateRef.current = onDrawCreate;

  useEffect(() => {
    const map = mapRef?.getMap();
    if (!map || drawRef.current) return;

    function init() {
      const draw = new TerraDraw({
        adapter: new TerraDrawMapLibreGLAdapter({ map: map! }),
        modes: [
          new TerraDrawPolygonMode(),
          new TerraDrawSelectMode({
            flags: {
              polygon: {
                feature: {
                  draggable: true,
                  coordinates: { midpoints: { draggable: true }, draggable: true, deletable: true },
                },
              },
            },
          }),
          new TerraDrawRenderMode({ modeName: 'static' } as any),
        ],
      });

      draw.start();
      draw.setMode('static');

      draw.on('finish', (id) => {
        const snapshot = draw.getSnapshot();
        const feature = snapshot.find((f) => f.id === id);
        if (feature?.geometry) {
          onDrawCreateRef.current(feature.geometry);
        }
      });

      drawRef.current = draw;
    }

    // Wait for style to load before initializing terra-draw
    if (map.isStyleLoaded()) {
      init();
    } else {
      map.once('style.load', init);
    }

    return () => {
      if (drawRef.current?.enabled) {
        drawRef.current.stop();
        drawRef.current = null;
      }
    };
  }, [mapRef]);

  useEffect(() => {
    if (!drawRef.current?.enabled) return;
    if (active) {
      drawRef.current.setMode('polygon');
    } else {
      drawRef.current.setMode('static');
      drawRef.current.clear();
      onDrawDelete();
    }
  }, [active]);

  return null;
}
