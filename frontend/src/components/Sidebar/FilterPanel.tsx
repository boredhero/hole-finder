import { useStore } from '../../store';
import { FEATURE_COLORS, FEATURE_LABELS } from '../../types';
import type { Basemap, FeatureType } from '../../types';

const ALL_TYPES: FeatureType[] = ['cave_entrance', 'mine_portal', 'sinkhole', 'depression', 'collapse_pit', 'spring', 'lava_tube', 'salt_dome_collapse', 'unknown'];
const BASEMAPS: { value: Basemap; label: string }[] = [
  { value: 'satellite', label: 'Satellite' },
  { value: 'lidar', label: 'LiDAR' },
  { value: 'topo', label: 'Topo' },
  { value: 'dark', label: 'Dark' },
];

export default function FilterPanel() {
  const filters = useStore((s) => s.filters);
  const setFilters = useStore((s) => s.setFilters);
  const basemap = useStore((s) => s.basemap);
  const setBasemap = useStore((s) => s.setBasemap);
  const showHeatmap = useStore((s) => s.showHeatmap);
  const toggleHeatmap = useStore((s) => s.toggleHeatmap);
  const showGroundTruth = useStore((s) => s.showGroundTruth);
  const toggleGroundTruth = useStore((s) => s.toggleGroundTruth);
  const show3DTerrain = useStore((s) => s.show3DTerrain);
  const toggle3DTerrain = useStore((s) => s.toggle3DTerrain);
  const terrainExaggeration = useStore((s) => s.terrainExaggeration);
  const setTerrainExaggeration = useStore((s) => s.setTerrainExaggeration);

  const toggleType = (ft: FeatureType) => {
    const types = filters.featureTypes.includes(ft)
      ? filters.featureTypes.filter(t => t !== ft)
      : [...filters.featureTypes, ft];
    setFilters({ featureTypes: types });
  };

  return (
    <div className="flex flex-col gap-5 p-5">
      {/* Basemap */}
      <section>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Basemap</h3>
        <div className="flex gap-1.5">
          {BASEMAPS.map(b => (
            <button key={b.value} onClick={() => setBasemap(b.value)}
              className={`flex-1 text-sm py-2 rounded-lg transition-colors font-medium ${basemap === b.value ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-300 hover:bg-slate-600'}`}>
              {b.label}
            </button>
          ))}
        </div>
      </section>

      {/* Feature Types */}
      <section>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Feature Types</h3>
        <div className="flex flex-col gap-1.5">
          {ALL_TYPES.map(ft => (
            <label key={ft} className="flex items-center gap-3 cursor-pointer text-sm py-1">
              <input type="checkbox" checked={filters.featureTypes.includes(ft)}
                onChange={() => toggleType(ft)}
                className="rounded border-slate-600 w-4 h-4" />
              <span className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: FEATURE_COLORS[ft] }} />
              <span className="text-slate-200">{FEATURE_LABELS[ft]}</span>
            </label>
          ))}
        </div>
      </section>

      {/* Confidence Range */}
      <section>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">
          Confidence: {filters.confidenceRange[0].toFixed(1)} - {filters.confidenceRange[1].toFixed(1)}
        </h3>
        <input type="range" min={0} max={1} step={0.05}
          value={filters.confidenceRange[0]}
          onChange={(e) => setFilters({ confidenceRange: [parseFloat(e.target.value), filters.confidenceRange[1]] })}
          className="w-full accent-blue-500 h-2" />
      </section>

      {/* Layer Toggles */}
      <section>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Layers</h3>
        <div className="flex flex-col gap-1.5">
          <label className="flex items-center gap-3 cursor-pointer text-sm py-1">
            <input type="checkbox" checked={showHeatmap} onChange={toggleHeatmap} className="rounded w-4 h-4" />
            <span className="text-slate-200">Heatmap</span>
          </label>
          <label className="flex items-center gap-3 cursor-pointer text-sm py-1">
            <input type="checkbox" checked={showGroundTruth} onChange={toggleGroundTruth} className="rounded w-4 h-4" />
            <span className="text-slate-200">Ground Truth Sites</span>
          </label>
          <label className="flex items-center gap-3 cursor-pointer text-sm py-1">
            <input type="checkbox" checked={show3DTerrain} onChange={toggle3DTerrain} className="rounded w-4 h-4" />
            <span className="text-slate-200">3D Terrain</span>
          </label>
        </div>
        {show3DTerrain && (
          <div className="ml-7 mt-2">
            <span className="text-sm text-slate-400">Exaggeration: {terrainExaggeration.toFixed(1)}x</span>
            <input type="range" min={0.5} max={5} step={0.5} value={terrainExaggeration}
              onChange={(e) => setTerrainExaggeration(parseFloat(e.target.value))}
              className="w-full accent-blue-500 h-2" />
          </div>
        )}
      </section>
    </div>
  );
}
