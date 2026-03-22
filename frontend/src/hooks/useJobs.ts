import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getJobs, createJob, cancelJob } from '../api/client';
import type { Job } from '../types';

export function useJobs() {
  return useQuery({
    queryKey: ['jobs'],
    queryFn: async () => {
      const data = await getJobs();
      return (data.jobs || []) as Job[];
    },
    refetchInterval: 5000,
  });
}

export function useCreateJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: createJob,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  });
}

export function useCancelJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: cancelJob,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['jobs'] }),
  });
}
