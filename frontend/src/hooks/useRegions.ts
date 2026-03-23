import { useQuery } from '@tanstack/react-query';
import { getRegions } from '../api/client';
import type { Region } from '../types';

export function useRegions() {
  return useQuery({
    queryKey: ['regions'],
    queryFn: async () => {
      const data = await getRegions();
      return (data.regions || []) as Region[];
    },
    staleTime: 300_000,
  });
}
