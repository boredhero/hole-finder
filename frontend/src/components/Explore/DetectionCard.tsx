import type { Detection } from '../../types';
import { FEATURE_COLORS, FEATURE_LABELS } from '../../types';
import { haversineDistance } from '../../utils';
import { MapPin, ExternalLink } from 'lucide-react';

interface DetectionCardProps {
  detection: Detection;
  compact?: boolean;
  userLocation?: { lat: number; lon: number } | null;
  selected?: boolean;
  onClick?: () => void;
}

export default function DetectionCard({ detection: d, compact, userLocation, selected, onClick }: DetectionCardProps) {
  const color = FEATURE_COLORS[d.feature_type] || '#6b7280';
  const label = FEATURE_LABELS[d.feature_type] || 'Unknown';
  const pct = Math.round(d.confidence * 100);
  const distance = userLocation ? haversineDistance(userLocation.lat, userLocation.lon, d.lat, d.lon) : null;

  const mapsUrl = `https://www.google.com/maps?q=${d.lat},${d.lon}`;

  if (compact) {
    return (
      <button
        onClick={onClick}
        className={`flex-shrink-0 w-52 snap-start rounded-xl p-3 text-left transition-all ${
          selected ? 'bg-slate-700 ring-2 ring-blue-500' : 'bg-slate-800 hover:bg-slate-750'
        }`}
      >
        <div className="flex items-center gap-2 mb-1.5">
          <span className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
          <span className="text-xs font-medium text-slate-200 truncate">{label}</span>
          <span className="ml-auto text-xs font-mono text-slate-400">{pct}%</span>
        </div>
        <div className="h-1 bg-slate-700 rounded-full overflow-hidden mb-2">
          <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: color }} />
        </div>
        <div className="flex items-center gap-2 text-xs text-slate-400">
          {d.depth_m != null && <span>{d.depth_m.toFixed(1)}m deep</span>}
          {distance != null && <span className="ml-auto">{distance.toFixed(1)} mi</span>}
        </div>
      </button>
    );
  }

  return (
    <div
      className={`rounded-xl p-4 transition-all ${
        selected ? 'bg-slate-700 ring-2 ring-blue-500' : 'bg-slate-800'
      }`}
    >
      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <span className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
        <span className="text-sm font-semibold text-white">{label}</span>
        <span className="ml-auto text-sm font-mono text-slate-300">{pct}%</span>
      </div>

      {/* Confidence bar */}
      <div className="h-1.5 bg-slate-700 rounded-full overflow-hidden mb-3">
        <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 gap-2 text-xs mb-3">
        {d.depth_m != null && (
          <div className="bg-slate-900/60 rounded-lg p-2">
            <div className="text-slate-500 uppercase text-xs">Depth</div>
            <div className="text-slate-100 font-mono">{d.depth_m.toFixed(1)} m</div>
          </div>
        )}
        {d.area_m2 != null && (
          <div className="bg-slate-900/60 rounded-lg p-2">
            <div className="text-slate-500 uppercase text-xs">Area</div>
            <div className="text-slate-100 font-mono">{d.area_m2.toFixed(0)} m{'\u00B2'}</div>
          </div>
        )}
        {d.circularity != null && (
          <div className="bg-slate-900/60 rounded-lg p-2">
            <div className="text-slate-500 uppercase text-xs">Circularity</div>
            <div className="text-slate-100 font-mono">{d.circularity.toFixed(2)}</div>
          </div>
        )}
        {distance != null && (
          <div className="bg-slate-900/60 rounded-lg p-2">
            <div className="text-slate-500 uppercase text-xs">Distance</div>
            <div className="text-slate-100 font-mono">{distance.toFixed(1)} mi</div>
          </div>
        )}
      </div>

      {/* Coordinates + Google Maps */}
      <div className="flex items-center gap-2 text-xs">
        <MapPin size={12} className="text-slate-500" />
        <span className="text-slate-400 font-mono">{d.lat.toFixed(5)}, {d.lon.toFixed(5)}</span>
        <a
          href={mapsUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="ml-auto flex items-center gap-1 bg-blue-600 hover:bg-blue-500 text-white px-2.5 py-1 rounded-lg transition-colors"
        >
          <ExternalLink size={11} />
          Open in Maps
        </a>
      </div>
    </div>
  );
}
