import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useStore } from '../../store';
import { validateDetection } from '../../api/client';
import { FEATURE_COLORS, FEATURE_LABELS } from '../../types';
import { ArrowLeft, CheckCircle, XCircle, HelpCircle } from 'lucide-react';

export default function DetailPanel() {
  const detection = useStore((s) => s.selectedDetection);
  const setSelectedDetection = useStore((s) => s.setSelectedDetection);
  const [notes, setNotes] = useState('');
  const qc = useQueryClient();

  const validate = useMutation({
    mutationFn: (verdict: string) => validateDetection(detection!.id, verdict, notes || undefined),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['detections'] });
      setNotes('');
    },
  });

  if (!detection) return <div className="p-4 text-slate-400 text-sm">No detection selected</div>;

  const m = detection.morphometrics || {};
  const color = FEATURE_COLORS[detection.feature_type] || '#6b7280';

  return (
    <div className="flex flex-col gap-3 p-4">
      {/* Header */}
      <div className="flex items-center gap-2">
        <button onClick={() => setSelectedDetection(null)} className="text-slate-400 hover:text-white">
          <ArrowLeft size={18} />
        </button>
        <span className="w-3 h-3 rounded-full" style={{ backgroundColor: color }} />
        <h3 className="font-semibold text-white text-sm">
          {FEATURE_LABELS[detection.feature_type] || 'Unknown'}
        </h3>
        <span className="ml-auto text-xs text-slate-400">
          {(detection.confidence * 100).toFixed(0)}%
        </span>
      </div>

      {/* Confidence bar */}
      <div className="h-1.5 bg-slate-700 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${detection.confidence * 100}%`, backgroundColor: color }} />
      </div>

      {/* Coordinates */}
      <div className="text-xs text-slate-400">
        {detection.lat.toFixed(5)}, {detection.lon.toFixed(5)}
      </div>

      {/* Morphometrics */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        {detection.depth_m != null && <Stat label="Depth" value={`${detection.depth_m.toFixed(1)} m`} />}
        {detection.area_m2 != null && <Stat label="Area" value={`${detection.area_m2.toFixed(0)} m\u00B2`} />}
        {detection.circularity != null && <Stat label="Circularity" value={detection.circularity.toFixed(2)} />}
        {detection.wall_slope_deg != null && <Stat label="Wall Slope" value={`${detection.wall_slope_deg.toFixed(1)}\u00B0`} />}
        {m.volume_m3 != null && <Stat label="Volume" value={`${Number(m.volume_m3).toFixed(0)} m\u00B3`} />}
        {m.k_parameter != null && <Stat label="k-param" value={Number(m.k_parameter).toFixed(2)} />}
        {m.elongation != null && <Stat label="Elongation" value={Number(m.elongation).toFixed(2)} />}
      </div>

      {/* Source passes */}
      {detection.source_passes && (
        <div className="text-xs">
          <span className="text-slate-400">Passes: </span>
          <span className="text-slate-200">
            {Array.isArray(detection.source_passes) ? detection.source_passes.join(', ') : JSON.stringify(detection.source_passes)}
          </span>
        </div>
      )}

      {/* Validation */}
      <section className="border-t border-slate-700 pt-3 mt-1">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">Validate</h4>
        {detection.validated != null && (
          <div className={`text-xs mb-2 ${detection.validated ? 'text-green-400' : 'text-red-400'}`}>
            Currently: {detection.validated ? 'Confirmed' : 'Rejected'}
          </div>
        )}
        <textarea
          value={notes} onChange={(e) => setNotes(e.target.value)}
          placeholder="Notes (optional)..."
          className="w-full bg-slate-800 border border-slate-600 rounded text-xs p-2 text-slate-200 resize-none h-16 mb-2"
        />
        <div className="flex gap-2">
          <button onClick={() => validate.mutate('confirmed')}
            className="flex-1 flex items-center justify-center gap-1 bg-green-700 hover:bg-green-600 text-white text-xs py-1.5 rounded transition-colors">
            <CheckCircle size={14} /> Confirm
          </button>
          <button onClick={() => validate.mutate('rejected')}
            className="flex-1 flex items-center justify-center gap-1 bg-red-700 hover:bg-red-600 text-white text-xs py-1.5 rounded transition-colors">
            <XCircle size={14} /> Reject
          </button>
          <button onClick={() => validate.mutate('uncertain')}
            className="flex-1 flex items-center justify-center gap-1 bg-slate-600 hover:bg-slate-500 text-white text-xs py-1.5 rounded transition-colors">
            <HelpCircle size={14} /> Unsure
          </button>
        </div>
      </section>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="bg-slate-800 rounded p-2">
      <div className="text-slate-400 text-[10px] uppercase">{label}</div>
      <div className="text-slate-100 font-mono text-sm">{value}</div>
    </div>
  );
}
