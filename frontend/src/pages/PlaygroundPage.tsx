import { Link } from 'react-router-dom';
import MapView from '../components/Map/MapView';
import Sidebar from '../components/Sidebar/Sidebar';
import { ArrowLeft } from 'lucide-react';

export default function PlaygroundPage() {
  return (
    <div className="relative h-full w-full">
      <div className="absolute inset-0">
        <MapView />
      </div>
      <Sidebar />
      <Link
        to="/"
        className="fixed top-3 right-3 z-50 bg-slate-800/90 backdrop-blur px-3 py-1.5 rounded-lg shadow-lg text-xs text-slate-300 hover:text-white flex items-center gap-1.5 transition-colors"
      >
        <ArrowLeft size={14} />
        Back to Explore
      </Link>
    </div>
  );
}
