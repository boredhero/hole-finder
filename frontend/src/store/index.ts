import { create } from 'zustand';
import type { Basemap, Detection, DetectionFilters } from '../types';

interface ViewState {
  longitude: number;
  latitude: number;
  zoom: number;
  pitch: number;
  bearing: number;
}

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
  terrainReady: boolean;
  setTerrainReady: (v: boolean) => void;
  terrainExaggeration: number;
  setTerrainExaggeration: (v: number) => void;
  showTileCoverage: boolean;
  toggleTileCoverage: () => void;

  // Map viewport persistence (survives route transitions)
  viewState: ViewState | null;
  setViewState: (v: ViewState) => void;
  targetViewState: ViewState | null;
  setTargetViewState: (v: ViewState) => void;
  clearTargetViewState: () => void;

  // Sidebar (playground)
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

  // Consumer explore view
  userLocation: { lat: number; lon: number } | null;
  setUserLocation: (loc: { lat: number; lon: number }) => void;
  drawerState: 'collapsed' | 'expanded' | 'detail';
  setDrawerState: (s: 'collapsed' | 'expanded' | 'detail') => void;
  searchBbox: [number, number, number, number] | null;
  setSearchBbox: (b: [number, number, number, number]) => void;
  searchStale: boolean;
  setSearchStale: (v: boolean) => void;

  // Consumer processing state
  activeJobId: string | null;
  setActiveJobId: (id: string | null) => void;
  processingStage: string | null;
  setProcessingStage: (s: string | null) => void;
  processingProgress: number;
  setProcessingProgress: (p: number) => void;

  // Guided tour state
  tourDetections: Detection[];
  setTourDetections: (d: Detection[]) => void;
  tourIndex: number;
  setTourIndex: (i: number) => void;
  tourActive: boolean;
  setTourActive: (v: boolean) => void;
}

export const useStore = create<AppState>((set) => ({
  basemap: 'satellite',
  setBasemap: (b) => set({ basemap: b }),
  showHeatmap: false,
  toggleHeatmap: () => set((s) => ({ showHeatmap: !s.showHeatmap })),
  showGroundTruth: true,
  toggleGroundTruth: () => set((s) => ({ showGroundTruth: !s.showGroundTruth })),
  show3DTerrain: true,
  toggle3DTerrain: () => set((s) => ({ show3DTerrain: !s.show3DTerrain })),
  terrainReady: false,
  setTerrainReady: (v) => set({ terrainReady: v }),
  terrainExaggeration: 1.5,
  setTerrainExaggeration: (v) => set({ terrainExaggeration: v }),
  showTileCoverage: false,
  toggleTileCoverage: () => set((s) => ({ showTileCoverage: !s.showTileCoverage })),

  viewState: null,
  setViewState: (v) => set({ viewState: v }),
  targetViewState: null,
  setTargetViewState: (v) => set({ targetViewState: v }),
  clearTargetViewState: () => set({ targetViewState: null }),

  sidebarOpen: true,
  setSidebarOpen: (open) => set({ sidebarOpen: open }),
  activePanel: 'filters',
  setActivePanel: (p) => set({ activePanel: p }),

  selectedDetection: null,
  setSelectedDetection: (d) => set({ selectedDetection: d, activePanel: d ? 'detail' : 'filters' }),
  hoveredDetectionId: null,
  setHoveredDetectionId: (id) => set({ hoveredDetectionId: id }),

  filters: {
    featureTypes: ['sinkhole', 'cave_entrance', 'mine_portal', 'depression', 'collapse_pit', 'spring', 'lava_tube', 'salt_dome_collapse', 'unknown'],
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

  userLocation: null,
  setUserLocation: (loc) => set({ userLocation: loc }),
  drawerState: 'collapsed',
  setDrawerState: (s) => set({ drawerState: s }),
  searchBbox: null,
  setSearchBbox: (b) => set({ searchBbox: b, searchStale: false }),
  searchStale: false,
  setSearchStale: (v) => set({ searchStale: v }),

  activeJobId: null,
  setActiveJobId: (id) => set({ activeJobId: id }),
  processingStage: null,
  setProcessingStage: (s) => set({ processingStage: s }),
  processingProgress: 0,
  setProcessingProgress: (p) => set({ processingProgress: p }),

  tourDetections: [],
  setTourDetections: (d) => set({ tourDetections: d }),
  tourIndex: 0,
  setTourIndex: (i) => set({ tourIndex: i }),
  tourActive: false,
  setTourActive: (v) => set({ tourActive: v }),
}));
