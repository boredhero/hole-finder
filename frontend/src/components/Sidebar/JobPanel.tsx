import { useState } from 'react';
import { useJobs, useCreateJob, useCancelJob } from '../../hooks/useJobs';
import { useStore } from '../../store';
import { Loader2, Play, X, PenTool, Clock, CheckCircle2, AlertCircle, Ban } from 'lucide-react';
import type { Job } from '../../types';

const CONFIGS = ['sinkhole_survey', 'cave_hunting', 'mine_detection'];
const REGIONS = ['western_pa', 'eastern_pa', 'west_virginia', 'eastern_ohio', 'upstate_ny'];

export default function JobPanel() {
  const { data: jobs = [], isLoading } = useJobs();
  const createJob = useCreateJob();
  const cancelJob = useCancelJob();
  const setDrawingAOI = useStore((s) => s.setDrawingAOI);
  const drawnAOI = useStore((s) => s.drawnAOI);
  const [region, setRegion] = useState(REGIONS[0]);
  const [config, setConfig] = useState(CONFIGS[0]);
  const [useDrawn, setUseDrawn] = useState(false);

  const activeJobs = jobs.filter((j: Job) => j.status === 'RUNNING' || j.status === 'PENDING');
  const completedJobs = jobs.filter((j: Job) => j.status !== 'RUNNING' && j.status !== 'PENDING');

  return (
    <div className="flex flex-col gap-4 p-4">
      {/* Active jobs — always visible at top */}
      {activeJobs.length > 0 && (
        <section>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-blue-400 mb-2 flex items-center gap-1">
            <Loader2 size={12} className="animate-spin" /> Active ({activeJobs.length})
          </h3>
          <div className="flex flex-col gap-2">
            {activeJobs.map((job: Job) => (
              <div key={job.id} className="bg-blue-950/50 border border-blue-800 rounded p-3">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs text-blue-200 font-medium">{job.job_type}</span>
                  <StatusBadge status={job.status} />
                </div>
                <div className="h-2 bg-slate-700 rounded-full overflow-hidden mb-1">
                  <div className="h-full bg-blue-500 rounded-full transition-all duration-500 ease-out"
                    style={{ width: `${Math.max(job.progress, 2)}%` }} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-blue-300">{job.progress.toFixed(0)}%</span>
                  <button onClick={() => cancelJob.mutate(job.id)}
                    className="text-red-400 hover:text-red-300 text-xs flex items-center gap-0.5">
                    <X size={10} /> Cancel
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Submit new job */}
      <section>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-2">New Job</h3>

        <div className="flex gap-1 mb-2">
          <button onClick={() => { setUseDrawn(false); setDrawingAOI(false); }}
            className={`flex-1 text-xs py-1.5 rounded ${!useDrawn ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-300'}`}>
            Region
          </button>
          <button onClick={() => { setUseDrawn(true); setDrawingAOI(true); }}
            className={`flex-1 text-xs py-1.5 rounded flex items-center justify-center gap-1 ${useDrawn ? 'bg-blue-600 text-white' : 'bg-slate-700 text-slate-300'}`}>
            <PenTool size={12} /> Draw AOI
          </button>
        </div>

        {!useDrawn ? (
          <select value={region} onChange={(e) => setRegion(e.target.value)}
            className="w-full bg-slate-800 border border-slate-600 rounded text-sm p-2 text-slate-200 mb-2">
            {REGIONS.map(r => <option key={r} value={r}>{r.replace(/_/g, ' ')}</option>)}
          </select>
        ) : (
          <div className="text-xs text-slate-400 mb-2 p-2 bg-slate-800 rounded">
            {drawnAOI ? '✓ AOI drawn on map' : 'Draw polygon on map...'}
          </div>
        )}

        <select value={config} onChange={(e) => setConfig(e.target.value)}
          className="w-full bg-slate-800 border border-slate-600 rounded text-sm p-2 text-slate-200 mb-2">
          {CONFIGS.map(c => <option key={c} value={c}>{c.replace(/_/g, ' ')}</option>)}
        </select>

        <button
          onClick={() => {
            const body: any = { job_type: 'full_pipeline', pass_config: config };
            if (useDrawn && drawnAOI) body.bbox = drawnAOI;
            else body.region_name = region;
            createJob.mutate(body);
            setDrawingAOI(false);
          }}
          disabled={createJob.isPending || (useDrawn && !drawnAOI)}
          className="w-full flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-sm py-2 rounded transition-colors">
          {createJob.isPending ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
          Submit Job
        </button>
      </section>

      {/* Completed jobs */}
      {completedJobs.length > 0 && (
        <section>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-2">
            History {isLoading && <Loader2 size={12} className="inline animate-spin ml-1" />}
          </h3>
          <div className="flex flex-col gap-1.5">
            {completedJobs.slice(0, 20).map((job: Job) => (
              <div key={job.id} className="bg-slate-800 rounded p-2.5">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-slate-300">{job.job_type}</span>
                  <StatusBadge status={job.status} />
                </div>
                {job.result_summary && (
                  <div className="text-xs text-slate-500 mt-0.5">
                    {(job.result_summary as any).total_detections != null &&
                      `${(job.result_summary as any).total_detections} detections`}
                    {(job.result_summary as any).tiles_downloaded != null &&
                      ` · ${(job.result_summary as any).tiles_downloaded} tiles`}
                  </div>
                )}
                {job.error_message && (
                  <div className="text-xs text-red-400 mt-0.5 truncate">{job.error_message}</div>
                )}
                <div className="text-xs text-slate-600 mt-0.5">{job.id.slice(0, 8)}</div>
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
    RUNNING: { bg: 'bg-blue-900/50 text-blue-300 border-blue-700', icon: Loader2 },
    COMPLETED: { bg: 'bg-green-900/50 text-green-300 border-green-700', icon: CheckCircle2 },
    FAILED: { bg: 'bg-red-900/50 text-red-300 border-red-700', icon: AlertCircle },
    CANCELLED: { bg: 'bg-slate-800 text-slate-400 border-slate-600', icon: Ban },
  };
  const c = config[status] || config.CANCELLED;
  const Icon = c.icon;
  return (
    <span className={`text-xs px-1.5 py-0.5 rounded border flex items-center gap-0.5 ${c.bg}`}>
      <Icon size={10} className={status === 'RUNNING' ? 'animate-spin' : ''} />
      {status}
    </span>
  );
}
