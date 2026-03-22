import { useQuery } from '@tanstack/react-query';
import { getDetections, getGroundTruth } from '../api/client';
import { useStore } from '../store';
import type { Detection, GroundTruthSite } from '../types';

export function useDetections() {
  const bbox = useStore((s) => s.bbox);
  const filters = useStore((s) => s.filters);

  return useQuery({
    queryKey: ['detections', bbox, filters],
    queryFn: async () => {
      if (!bbox) return [];
      const data = await getDetections({
        west: bbox[0], south: bbox[1], east: bbox[2], north: bbox[3],
        min_confidence: filters.confidenceRange[0],
        feature_type: filters.featureTypes,
        limit: 10000,
      });
      return (data.features || []).map((f: any) => ({
        id: f.id,
        lon: f.geometry.coordinates[0],
        lat: f.geometry.coordinates[1],
        ...f.properties,
      })) as Detection[];
    },
    enabled: !!bbox,
    staleTime: 30_000,
  });
}

export function useGroundTruth() {
  const bbox = useStore((s) => s.bbox);

  return useQuery({
    queryKey: ['groundTruth', bbox],
    queryFn: async () => {
      const params = bbox ? { west: bbox[0], south: bbox[1], east: bbox[2], north: bbox[3] } : undefined;
      const data = await getGroundTruth(params);
      return (data.features || []).map((f: any) => ({
        id: f.id,
        name: f.properties.name,
        lon: f.geometry.coordinates[0],
        lat: f.geometry.coordinates[1],
        feature_type: f.properties.feature_type,
        source: f.properties.source,
      })) as GroundTruthSite[];
    },
    staleTime: 120_000,
  });
}
