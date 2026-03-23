import { useEffect, useRef } from 'react';
import MapboxDraw from '@mapbox/mapbox-gl-draw';
import '@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css';
import { useControl } from 'react-map-gl/maplibre';

interface DrawControlProps {
  onDrawCreate: (geometry: GeoJSON.Geometry) => void;
  onDrawDelete: () => void;
  active: boolean;
}

export default function DrawControl({ onDrawCreate, onDrawDelete, active }: DrawControlProps) {
  const drawRef = useRef<any>(null);

  useControl(
    () => {
      const d = new MapboxDraw({
        displayControlsDefault: false,
        controls: { polygon: true, trash: true },
        defaultMode: 'simple_select',
      });
      drawRef.current = d;
      return d;
    },
    ({ map }) => {
      map.on('draw.create', (e: any) => {
        const feature = e.features?.[0];
        if (feature?.geometry) {
          onDrawCreate(feature.geometry);
        }
      });
      map.on('draw.delete', () => onDrawDelete());
    },
    () => {},
  );

  useEffect(() => {
    if (drawRef.current) {
      if (active) {
        drawRef.current.changeMode('draw_polygon');
      } else {
        drawRef.current.changeMode('simple_select');
        drawRef.current.deleteAll();
      }
    }
  }, [active]);

  return null;
}
