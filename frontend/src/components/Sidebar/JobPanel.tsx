import { useState } from 'react';
import { useJobs, useCreateJob, useCancelJob } from '../../hooks/useJobs';
import { Loader2, Play, X } from 'lucide-react';

const CONFIGS = ['sinkhole_survey', 'cave_hunting', 'mine_detection'];
const REGIONS = ['western_pa', 'eastern_pa', 'west_virginia', 'eastern_ohio', 'upstate_ny'];

export default function JobPanel() {
  const { data: jobs = [], isLoading } = useJobs();
  const createJob = useCreateJob();
  const cancelJob = useCancelJob();
  const [region, setRegion] = useState(REGIONS[0]);
  const [config, setConfig] = useState(CONFIGS[0]);

  return (
    <div className="flex flex-col gap-4 p-4">
      {/* Submit new job */}
      <section>
        <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">New Job</h3>
        <select value={region} onChange={(e) => setRegion(e.target.value)}
          className="w-full bg-slate-800 border border-slate-600 rounded text-sm p-2 text-slate-200 mb-2">
          {REGIONS.map(r => <option key={r} value={r}>{r.replace(/_/g, ' ')}</option>)}
        </select>
        <select value={config} onChange={(e) => setConfig(e.target.value)}
          className="w-full bg-slate-800 border border-slate-600 rounded text-sm p-2 text-slate-200 mb-2">
          {CONFIGS.map(c => <option key={c} value={c}>{c.replace(/_/g, ' ')}</option>)}
        </select>
        <button
          onClick={() => createJob.mutate({ job_type: 'full_pipeline', region_name: region, pass_config: config })}
          disabled={createJob.isPending}
          className="w-full flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-sm py-2 rounded transition-colors">
          {createJob.isPending ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
          Submit Job
        </button>
      </section>

      {/* Job list */}
      <section>
        <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2">
          Jobs {isLoading && <Loader2 size={12} className="inline animate-spin ml-1" />}
        </h3>
        {jobs.length === 0 ? (
          <p className="text-slate-500 text-xs">No jobs yet</p>
        ) : (
          <div className="flex flex-col gap-2">
            {jobs.map((job) => (
              <div key={job.id} className="bg-slate-800 rounded p-3">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs text-slate-200 font-medium">{job.job_type}</span>
                  <StatusBadge status={job.status} />
                </div>
                {/* Progress bar */}
                {(job.status === 'RUNNING' || job.status === 'PENDING') && (
                  <div className="h-1 bg-slate-700 rounded-full overflow-hidden mb-1">
                    <div className="h-full bg-blue-500 rounded-full transition-all" style={{ width: `${job.progress}%` }} />
                  </div>
                )}
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-slate-500">{job.id.slice(0, 8)}...</span>
                  {(job.status === 'PENDING' || job.status === 'RUNNING') && (
                    <button onClick={() => cancelJob.mutate(job.id)}
                      className="text-red-400 hover:text-red-300 text-[10px] flex items-center gap-0.5">
                      <X size={10} /> Cancel
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    PENDING: 'bg-yellow-900 text-yellow-300',
    RUNNING: 'bg-blue-900 text-blue-300',
    COMPLETED: 'bg-green-900 text-green-300',
    FAILED: 'bg-red-900 text-red-300',
    CANCELLED: 'bg-slate-700 text-slate-400',
  };
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded ${colors[status] || 'bg-slate-700 text-slate-400'}`}>
      {status}
    </span>
  );
}
