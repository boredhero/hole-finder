import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useStore } from '../../store';
import { validateDetection } from '../../api/client';
import { FEATURE_COLORS, FEATURE_LABELS } from '../../types';
import { ArrowLeft, CheckCircle, XCircle, HelpCircle, MessageSquare, Bookmark, Send } from 'lucide-react';

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

  if (!detection) return <div className="p-6 text-slate-400 text-base">No detection selected</div>;

  const m = detection.morphometrics || {};
  const color = FEATURE_COLORS[detection.feature_type] || '#6b7280';

  return (
    <div className="flex flex-col gap-4 p-5">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button onClick={() => setSelectedDetection(null)} className="text-slate-400 hover:text-white">
          <ArrowLeft size={22} />
        </button>
        <span className="w-4 h-4 rounded-full" style={{ backgroundColor: color }} />
        <h3 className="font-semibold text-white text-base">
          {FEATURE_LABELS[detection.feature_type] || 'Unknown'}
        </h3>
        <span className="ml-auto text-sm font-mono text-slate-300">
          {(detection.confidence * 100).toFixed(0)}%
        </span>
      </div>

      {/* Confidence bar */}
      <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${detection.confidence * 100}%`, backgroundColor: color }} />
      </div>

      {/* Coordinates */}
      <div className="text-sm text-slate-400 font-mono">
        {detection.lat.toFixed(5)}, {detection.lon.toFixed(5)}
      </div>

      {/* Morphometrics */}
      <div className="grid grid-cols-2 gap-3">
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
        <div className="text-sm">
          <span className="text-slate-400">Passes: </span>
          <span className="text-slate-200">
            {Array.isArray(detection.source_passes) ? detection.source_passes.join(', ') : JSON.stringify(detection.source_passes)}
          </span>
        </div>
      )}

      {/* Validation */}
      <section className="border-t border-slate-700 pt-4 mt-2">
        <h4 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">Validate</h4>
        {detection.validated != null && (
          <div className={`text-sm mb-3 ${detection.validated ? 'text-green-400' : 'text-red-400'}`}>
            Currently: {detection.validated ? 'Confirmed' : 'Rejected'}
          </div>
        )}
        <textarea
          value={notes} onChange={(e) => setNotes(e.target.value)}
          placeholder="Notes (optional)..."
          className="w-full bg-slate-800 border border-slate-600 rounded-lg text-sm p-3 text-slate-200 resize-none h-20 mb-3"
        />
        <div className="flex gap-2.5">
          <button onClick={() => validate.mutate('confirmed')}
            className="flex-1 flex items-center justify-center gap-2 bg-green-700 hover:bg-green-600 text-white text-sm py-2.5 rounded-lg transition-colors font-medium">
            <CheckCircle size={16} /> Confirm
          </button>
          <button onClick={() => validate.mutate('rejected')}
            className="flex-1 flex items-center justify-center gap-2 bg-red-700 hover:bg-red-600 text-white text-sm py-2.5 rounded-lg transition-colors font-medium">
            <XCircle size={16} /> Reject
          </button>
          <button onClick={() => validate.mutate('uncertain')}
            className="flex-1 flex items-center justify-center gap-2 bg-slate-600 hover:bg-slate-500 text-white text-sm py-2.5 rounded-lg transition-colors font-medium">
            <HelpCircle size={16} /> Unsure
          </button>
        </div>
      </section>

      {/* Save / Highlight */}
      <section className="border-t border-slate-700 pt-4 mt-2">
        <button
          onClick={() => {
            fetch(`/api/detections/${detection.id}/save`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ label: 'Interesting', color: '#f59e0b' }),
            }).then(() => qc.invalidateQueries({ queryKey: ['saved'] }));
          }}
          className="w-full flex items-center justify-center gap-2 bg-amber-700 hover:bg-amber-600 text-white text-sm py-2.5 rounded-lg transition-colors font-medium">
          <Bookmark size={16} /> Save Detection
        </button>
      </section>

      {/* Comments */}
      <CommentsSection detectionId={detection.id} />
    </div>
  );
}

function CommentsSection({ detectionId }: { detectionId: string }) {
  const [text, setText] = useState('');
  const [author, setAuthor] = useState('');
  const qc = useQueryClient();

  const { data: comments = [] } = useQuery({
    queryKey: ['comments', detectionId],
    queryFn: async () => {
      const res = await fetch(`/api/detections/${detectionId}/comments`);
      return res.json();
    },
  });

  const addComment = useMutation({
    mutationFn: async () => {
      await fetch(`/api/detections/${detectionId}/comments`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text, author: author || 'Anonymous' }),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['comments', detectionId] });
      setText('');
    },
  });

  return (
    <section className="border-t border-slate-700 pt-4 mt-2">
      <h4 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3 flex items-center gap-2">
        <MessageSquare size={16} /> Comments ({comments.length})
      </h4>

      {comments.map((c: any) => (
        <div key={c.id} className="bg-slate-800 rounded-lg p-3 mb-2">
          <div className="text-sm text-slate-200">{c.text}</div>
          <div className="text-xs text-slate-500 mt-1">{c.author} &middot; {new Date(c.created_at).toLocaleDateString()}</div>
        </div>
      ))}

      <div className="flex gap-2 mt-3">
        <input value={author} onChange={(e) => setAuthor(e.target.value)}
          placeholder="Name" className="w-20 bg-slate-800 border border-slate-600 rounded-lg text-sm p-2.5 text-slate-200" />
        <input value={text} onChange={(e) => setText(e.target.value)}
          placeholder="Add comment..." className="flex-1 bg-slate-800 border border-slate-600 rounded-lg text-sm p-2.5 text-slate-200"
          onKeyDown={(e) => e.key === 'Enter' && text && addComment.mutate()} />
        <button onClick={() => text && addComment.mutate()} disabled={!text}
          className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white px-3 rounded-lg">
          <Send size={16} />
        </button>
      </div>
    </section>
  );
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="bg-slate-800 rounded-lg p-3">
      <div className="text-slate-400 text-xs uppercase tracking-wide">{label}</div>
      <div className="text-slate-100 font-mono text-base mt-0.5">{value}</div>
    </div>
  );
}
