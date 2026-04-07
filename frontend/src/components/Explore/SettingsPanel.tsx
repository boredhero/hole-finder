import { useState, useEffect } from 'react';
import { Settings, X, MapPin } from 'lucide-react';
import { useStore } from '../../store';
import { FEATURE_COLORS, FEATURE_LABELS } from '../../types';
import type { Basemap, FeatureType } from '../../types';

const ALL_TYPES: FeatureType[] = [
  'cave_entrance', 'mine_portal', 'sinkhole', 'depression',
  'collapse_pit', 'spring', 'lava_tube', 'salt_dome_collapse', 'unknown',
];

const BASEMAPS: { value: Basemap; label: string }[] = [
  { value: 'satellite', label: 'Satellite' },
  { value: 'relief', label: 'Relief' },
  { value: 'topo', label: 'Topo' },
  { value: 'dark', label: 'Dark' },
];

function ToggleSwitch({ enabled, color, onToggle }: { enabled: boolean; color: string; onToggle: () => void }) {
  return (
    <button
      onClick={onToggle}
      className="relative w-11 h-6 rounded-full transition-colors flex-shrink-0"
      style={{ backgroundColor: enabled ? color : '#444444' }}
    >
      <span
        className="absolute top-0.5 w-5 h-5 bg-white rounded-full transition-all shadow"
        style={{ left: enabled ? '22px' : '2px' }}
      />
    </button>
  );
}

export default function SettingsPanel() {
  const [open, setOpen] = useState(() => window.innerWidth >= 768);

  const basemap = useStore((s) => s.basemap);
  const setBasemap = useStore((s) => s.setBasemap);
  const showGroundTruth = useStore((s) => s.showGroundTruth);
  const toggleGroundTruth = useStore((s) => s.toggleGroundTruth);
  const filters = useStore((s) => s.filters);
  const setFilters = useStore((s) => s.setFilters);

  // Close panel on resize to mobile
  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth < 768) setOpen(false);
    };
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  const toggleType = (ft: FeatureType) => {
    const types = filters.featureTypes.includes(ft)
      ? filters.featureTypes.filter(t => t !== ft)
      : [...filters.featureTypes, ft];
    setFilters({ featureTypes: types });
  };

  // Gear icon toggle button (always visible)
  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="fixed top-16 left-4 z-30 bg-slate-900/90 backdrop-blur-lg border border-slate-700 p-3 rounded shadow-lg text-slate-300 hover:text-white transition-colors"
      >
        <Settings size={20} />
      </button>
    );
  }

  return (
    <div className="fixed top-16 left-4 z-30 w-64 md:w-72 max-h-[85vh] overflow-y-auto bg-slate-900/90 backdrop-blur-lg border border-slate-700 rounded shadow-xl">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700">
        <h3 className="text-sm font-bold text-white uppercase tracking-wider">Settings</h3>
        <button onClick={() => setOpen(false)} className="text-slate-400 hover:text-white transition-colors">
          <X size={18} />
        </button>
      </div>

      <div className="flex flex-col gap-5 px-5 py-4">
        {/* How to use */}
        <section>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">How to use</h4>
          <ul className="text-xs text-slate-400 space-y-1.5">
            <li>Pinch or scroll to zoom in/out</li>
            <li>Tap a pink shape for detection details</li>
            <li>Swipe cards left/right to browse finds</li>
            <li>Hit "Search this area" to load card list</li>
          </ul>
        </section>

        {/* Map style */}
        <section>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Map style</h4>
          <div className="grid grid-cols-2 gap-2">
            {BASEMAPS.map((b) => (
              <button
                key={b.value}
                onClick={() => setBasemap(b.value)}
                className={`text-sm py-2.5 px-3 rounded transition-colors font-medium ${
                  basemap === b.value
                    ? 'bg-burgundy-500 text-white'
                    : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
                }`}
              >
                {b.label}
              </button>
            ))}
          </div>
        </section>

        {/* Overlays */}
        <section>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Overlays</h4>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-sm text-slate-200">
              <MapPin size={14} className="text-yellow-400" />
              Ground Truth
            </div>
            <ToggleSwitch enabled={showGroundTruth} color="#eab308" onToggle={toggleGroundTruth} />
          </div>
        </section>

        {/* Confidence */}
        <section>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">
            Confidence: {filters.confidenceRange[0].toFixed(1)}+
          </h4>
          <input
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={filters.confidenceRange[0]}
            onChange={(e) => setFilters({ confidenceRange: [parseFloat(e.target.value), filters.confidenceRange[1]] })}
            className="w-full accent-cherry-500 h-2"
          />
          <div className="flex justify-between text-xs text-slate-600 mt-1">
            <span>0.0</span>
            <span>1.0</span>
          </div>
        </section>

        {/* Feature types */}
        <section>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-3">Feature types</h4>
          <div className="flex flex-col gap-2.5">
            {ALL_TYPES.map((ft) => (
              <div key={ft} className="flex items-center justify-between">
                <div className="flex items-center gap-2.5">
                  <span
                    className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                    style={{ backgroundColor: FEATURE_COLORS[ft] }}
                  />
                  <span className="text-sm text-slate-200">{FEATURE_LABELS[ft]}</span>
                </div>
                <ToggleSwitch
                  enabled={filters.featureTypes.includes(ft)}
                  color={FEATURE_COLORS[ft]}
                  onToggle={() => toggleType(ft)}
                />
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
