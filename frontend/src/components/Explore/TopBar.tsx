import { Link } from 'react-router-dom';
import { useStore } from '../../store';
import type { Basemap } from '../../types';
import { Flame, MapPin, Settings2 } from 'lucide-react';

const BASEMAPS: { value: Basemap; label: string }[] = [
  { value: 'satellite', label: 'Sat' },
  { value: 'lidar', label: 'LiDAR' },
  { value: 'topo', label: 'Topo' },
  { value: 'dark', label: 'Dark' },
];

export default function TopBar() {
  const basemap = useStore((s) => s.basemap);
  const setBasemap = useStore((s) => s.setBasemap);
  const showHeatmap = useStore((s) => s.showHeatmap);
  const toggleHeatmap = useStore((s) => s.toggleHeatmap);
  const showGroundTruth = useStore((s) => s.showGroundTruth);
  const toggleGroundTruth = useStore((s) => s.toggleGroundTruth);

  return (
    <div className="fixed top-0 inset-x-0 z-30 bg-slate-900/80 backdrop-blur-lg border-b border-slate-700/50">
      <div className="flex items-center gap-3 px-4 py-2.5">
        {/* Title */}
        <span className="text-sm font-bold text-white tracking-wide whitespace-nowrap">HOLE FINDER</span>

        {/* Basemap pills */}
        <div className="flex gap-0.5 bg-slate-800/80 rounded-lg p-0.5 ml-3">
          {BASEMAPS.map((b) => (
            <button
              key={b.value}
              onClick={() => setBasemap(b.value)}
              className={`px-2 py-1 text-xs rounded-md transition-colors ${
                basemap === b.value ? 'bg-blue-600 text-white' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {b.label}
            </button>
          ))}
        </div>

        {/* Layer toggles */}
        <div className="flex gap-1 ml-2">
          <button
            onClick={toggleHeatmap}
            className={`p-1.5 rounded-md transition-colors ${showHeatmap ? 'bg-orange-600/30 text-orange-400' : 'text-slate-500 hover:text-slate-300'}`}
            title="Heatmap"
          >
            <Flame size={14} />
          </button>
          <button
            onClick={toggleGroundTruth}
            className={`p-1.5 rounded-md transition-colors ${showGroundTruth ? 'bg-yellow-600/30 text-yellow-400' : 'text-slate-500 hover:text-slate-300'}`}
            title="Ground truth sites"
          >
            <MapPin size={14} />
          </button>
        </div>

        {/* Spacer */}
        <div className="flex-1" />

        {/* Playground link */}
        <Link
          to="/playground"
          className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-white transition-colors"
        >
          <Settings2 size={13} />
          Advanced
        </Link>
      </div>
    </div>
  );
}
