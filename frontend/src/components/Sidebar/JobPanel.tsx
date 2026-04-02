import { useState } from 'react';
import { useJobs, useCreateJob, useCancelJob } from '../../hooks/useJobs';
import { useStore } from '../../store';
import { Loader2, Play, X, PenTool, Clock, CheckCircle2, AlertCircle, Ban, MapPin } from 'lucide-react';
import type { Job } from '../../types';

const CONFIGS = ['sinkhole_survey', 'cave_hunting', 'mine_detection', 'salt_dome_detection', 'lava_tube_detection'];

export default function JobPanel() {
  const { data: jobs = [], isLoading } = useJobs();
  const createJob = useCreateJob();
  const cancelJob = useCancelJob();
  const setDrawingAOI = useStore((s) => s.setDrawingAOI);
  const drawnAOI = useStore((s) => s.drawnAOI);
  const [config, setConfig] = useState(CONFIGS[0]);
  const [inputMode, setInputMode] = useState<'draw' | 'pin'>('pin');
  const [pinLat, setPinLat] = useState('');
  const [pinLon, setPinLon] = useState('');
  const [pinRadius, setPinRadius] = useState(2);
  const activeJobs = jobs.filter((j: Job) => j.status === 'RUNNING' || j.status === 'PENDING');
  const completedJobs = jobs.filter((j: Job) => j.status !== 'RUNNING' && j.status !== 'PENDING');
  return (
    <div className="flex flex-col gap-5 p-6">
      {activeJobs.length > 0 && (
        <section>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-amaranth-400 mb-3 flex items-center gap-2">
            <Loader2 size={16} className="animate-spin" /> Active ({activeJobs.length})
          </h3>
          <div className="flex flex-col gap-3">
            {activeJobs.map((job: Job) => (
              <div key={job.id} className="bg-amaranth-950/50 border border-amaranth-800 rounded p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-amaranth-200 font-medium">{job.job_type.replace(/_/g, ' ')}</span>
                  <StatusBadge status={job.status} />
                </div>
                <div className="h-3 bg-slate-700 rounded overflow-hidden mb-2">
                  <div className="h-full bg-hotpink-500 rounded transition-all duration-500 ease-out" style={{ width: `${Math.max(job.progress, 2)}%` }} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm font-mono text-amaranth-300">{job.progress.toFixed(0)}%</span>
                  <button onClick={() => cancelJob.mutate(job.id)} className="text-red-400 hover:text-red-300 text-sm flex items-center gap-1">
                    <X size={14} /> Cancel
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>
      )}
      <section>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">New Job</h3>
        <div className="flex gap-2 mb-4">
          <button onClick={() => { setInputMode('pin'); setDrawingAOI(false); }}
            className={`flex-1 text-sm py-3 px-3 rounded font-medium flex items-center justify-center gap-1.5 transition-colors ${inputMode === 'pin' ? 'bg-burgundy-500 text-white' : 'bg-slate-700 text-slate-300 hover:bg-slate-600'}`}>
            <MapPin size={14} /> Pin
          </button>
          <button onClick={() => { setInputMode('draw'); setDrawingAOI(true); }}
            className={`flex-1 text-sm py-3 px-3 rounded font-medium flex items-center justify-center gap-1.5 transition-colors ${inputMode === 'draw' ? 'bg-burgundy-500 text-white' : 'bg-slate-700 text-slate-300 hover:bg-slate-600'}`}>
            <PenTool size={14} /> Draw
          </button>
        </div>
        {inputMode === 'pin' && (
          <div className="flex flex-col gap-2 mb-3">
            <div className="flex gap-2">
              <input value={pinLat} onChange={(e) => setPinLat(e.target.value)} placeholder="Latitude"
                className="flex-1 bg-slate-800 border border-slate-600 rounded text-sm p-3 text-slate-200" type="number" step="0.001" />
              <input value={pinLon} onChange={(e) => setPinLon(e.target.value)} placeholder="Longitude"
                className="flex-1 bg-slate-800 border border-slate-600 rounded text-sm p-3 text-slate-200" type="number" step="0.001" />
            </div>
            <div className="flex items-center gap-3">
              <span className="text-sm text-slate-400 whitespace-nowrap">Radius: {pinRadius} km</span>
              <input type="range" min={0.5} max={10} step={0.5} value={pinRadius}
                onChange={(e) => setPinRadius(parseFloat(e.target.value))}
                className="flex-1 accent-cherry-500 h-2" />
            </div>
          </div>
        )}
        {inputMode === 'draw' && (
          <div className="text-sm text-slate-400 mb-3 p-3 bg-slate-800 rounded">
            {drawnAOI ? 'AOI drawn on map' : 'Draw polygon on map...'}
          </div>
        )}
        <select value={config} onChange={(e) => setConfig(e.target.value)}
          className="w-full bg-slate-800 border border-slate-600 rounded text-sm p-3 text-slate-200 mb-3">
          {CONFIGS.map(c => <option key={c} value={c}>{c.replace(/_/g, ' ')}</option>)}
        </select>
        <button
          onClick={() => {
            const body: any = { job_type: 'full_pipeline', pass_config: config };
            if (inputMode === 'draw' && drawnAOI) {
              body.bbox = drawnAOI;
            } else if (inputMode === 'pin' && pinLat && pinLon) {
              const lat = parseFloat(pinLat);
              const lon = parseFloat(pinLon);
              const r = pinRadius / 111.32;
              body.bbox = { type: 'Polygon', coordinates: [[[lon-r, lat-r], [lon+r, lat-r], [lon+r, lat+r], [lon-r, lat+r], [lon-r, lat-r]]] };
              body.center_lat = lat;
              body.center_lon = lon;
            }
            createJob.mutate(body);
            setDrawingAOI(false);
          }}
          disabled={createJob.isPending || (inputMode === 'draw' && !drawnAOI) || (inputMode === 'pin' && (!pinLat || !pinLon))}
          className="w-full flex items-center justify-center gap-2 bg-cherry-500 hover:bg-cherry-400 disabled:opacity-50 text-white text-sm font-medium py-3.5 px-4 rounded transition-colors">
          {createJob.isPending ? <Loader2 size={18} className="animate-spin" /> : <Play size={18} />}
          Submit Job
        </button>
      </section>
      {completedJobs.length > 0 && (
        <section>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-3">
            History {isLoading && <Loader2 size={14} className="inline animate-spin ml-1" />}
          </h3>
          <div className="flex flex-col gap-2">
            {completedJobs.slice(0, 20).map((job: Job) => (
              <div key={job.id} className="bg-slate-800 rounded p-3.5">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-slate-300">{job.job_type.replace(/_/g, ' ')}</span>
                  <StatusBadge status={job.status} />
                </div>
                {job.result_summary && (
                  <div className="text-sm text-slate-500 mt-1">
                    {(job.result_summary as any).total_detections != null && `${(job.result_summary as any).total_detections} detections`}
                    {(job.result_summary as any).tiles_downloaded != null && ` · ${(job.result_summary as any).tiles_downloaded} tiles`}
                  </div>
                )}
                {job.error_message && (<div className="text-sm text-red-400 mt-1 truncate">{job.error_message}</div>)}
                <div className="text-xs text-slate-600 mt-1 font-mono">{job.id.slice(0, 8)}</div>
              </div>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const config: Record<string, { bg: string; icon: any }> = {
    PENDING: { bg: 'bg-yellow-900/50 text-yellow-300 border-yellow-700', icon: Clock },
    RUNNING: { bg: 'bg-amaranth-900/50 text-amaranth-300 border-amaranth-700', icon: Loader2 },
    COMPLETED: { bg: 'bg-green-900/50 text-green-300 border-green-700', icon: CheckCircle2 },
    FAILED: { bg: 'bg-red-900/50 text-red-300 border-red-700', icon: AlertCircle },
    CANCELLED: { bg: 'bg-slate-800 text-slate-400 border-slate-600', icon: Ban },
  };
  const c = config[status] || config.CANCELLED;
  const Icon = c.icon;
  return (
    <span className={`text-sm px-3 py-1.5 rounded border flex items-center gap-1.5 ${c.bg}`}>
      <Icon size={13} className={status === 'RUNNING' ? 'animate-spin' : ''} />
      {status}
    </span>
  );
}
