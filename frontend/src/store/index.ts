import { create } from 'zustand';
import type { Basemap, Detection, DetectionFilters } from '../types';

interface AppState {
  // Map state
  basemap: Basemap;
  setBasemap: (b: Basemap) => void;
  showHeatmap: boolean;
  toggleHeatmap: () => void;
  showGroundTruth: boolean;
  toggleGroundTruth: () => void;
  show3DTerrain: boolean;
  toggle3DTerrain: () => void;
  terrainExaggeration: number;
  setTerrainExaggeration: (v: number) => void;

  // Sidebar
  sidebarOpen: boolean;
  setSidebarOpen: (open: boolean) => void;
  activePanel: 'filters' | 'detections' | 'jobs' | 'detail';
  setActivePanel: (p: 'filters' | 'detections' | 'jobs' | 'detail') => void;

  // Detection selection
  selectedDetection: Detection | null;
  setSelectedDetection: (d: Detection | null) => void;
  hoveredDetectionId: string | null;
  setHoveredDetectionId: (id: string | null) => void;

  // Filters
  filters: DetectionFilters;
  setFilters: (f: Partial<DetectionFilters>) => void;

  // Map viewport for queries
  bbox: [number, number, number, number] | null;
  setBbox: (b: [number, number, number, number]) => void;

  // AOI drawing
  drawingAOI: boolean;
  setDrawingAOI: (v: boolean) => void;
  drawnAOI: GeoJSON.Geometry | null;
  setDrawnAOI: (g: GeoJSON.Geometry | null) => void;
}

export const useStore = create<AppState>((set) => ({
  basemap: 'dark',
  setBasemap: (b) => set({ basemap: b }),
  showHeatmap: false,
  toggleHeatmap: () => set((s) => ({ showHeatmap: !s.showHeatmap })),
  showGroundTruth: true,
  toggleGroundTruth: () => set((s) => ({ showGroundTruth: !s.showGroundTruth })),
  show3DTerrain: false,
  toggle3DTerrain: () => set((s) => ({ show3DTerrain: !s.show3DTerrain })),
  terrainExaggeration: 1.5,
  setTerrainExaggeration: (v) => set({ terrainExaggeration: v }),

  sidebarOpen: true,
  setSidebarOpen: (open) => set({ sidebarOpen: open }),
  activePanel: 'filters',
  setActivePanel: (p) => set({ activePanel: p }),

  selectedDetection: null,
  setSelectedDetection: (d) => set({ selectedDetection: d, activePanel: d ? 'detail' : 'filters' }),
  hoveredDetectionId: null,
  setHoveredDetectionId: (id) => set({ hoveredDetectionId: id }),

  filters: {
    featureTypes: ['sinkhole', 'cave_entrance', 'mine_portal', 'depression', 'collapse_pit', 'spring', 'unknown'],
    confidenceRange: [0.3, 1.0],
    validated: null,
  },
  setFilters: (f) => set((s) => ({ filters: { ...s.filters, ...f } })),

  bbox: null,
  setBbox: (b) => set({ bbox: b }),

  drawingAOI: false,
  setDrawingAOI: (v) => set({ drawingAOI: v }),
  drawnAOI: null,
  setDrawnAOI: (g) => set({ drawnAOI: g }),
}));
