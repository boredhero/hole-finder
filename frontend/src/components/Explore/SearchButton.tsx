import { useStore } from '../../store';
import { Search } from 'lucide-react';

export default function SearchButton() {
  const searchStale = useStore((s) => s.searchStale);
  const bbox = useStore((s) => s.bbox);
  const setSearchBbox = useStore((s) => s.setSearchBbox);

  if (!searchStale || !bbox) return null;

  return (
    <button
      onClick={() => setSearchBbox(bbox)}
      className="fixed top-16 left-1/2 -translate-x-1/2 z-30 bg-blue-600 hover:bg-blue-500 text-white font-medium text-sm px-5 py-2.5 rounded-full shadow-lg flex items-center gap-2 transition-all animate-in fade-in"
    >
      <Search size={16} />
      Search this area
    </button>
  );
}
