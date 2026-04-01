import { useEffect } from 'react';
import { Link } from 'react-router-dom';
import MapView from '../components/Map/MapView';
import Sidebar from '../components/Sidebar/Sidebar';
import { useStore } from '../store';
import { ArrowLeft } from 'lucide-react';

export default function PlaygroundPage() {
  const setTerrainReady = useStore((s) => s.setTerrainReady);
  useEffect(() => { setTerrainReady(true); }, [setTerrainReady]);
  return (
    <div className="relative h-full w-full">
      <div className="absolute inset-0">
        <MapView />
      </div>
      <Sidebar />
      <Link
        to="/"
        className="fixed top-4 right-4 z-50 bg-slate-800/90 backdrop-blur px-5 py-3 rounded shadow-lg text-sm text-slate-300 hover:text-white flex items-center gap-2 transition-colors"
      >
        <ArrowLeft size={16} />
        Back to Explore
      </Link>
    </div>
  );
}
