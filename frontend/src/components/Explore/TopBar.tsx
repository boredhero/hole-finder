import { Link } from 'react-router-dom';
import { Settings2 } from 'lucide-react';

export default function TopBar() {
  return (
    <div className="fixed top-0 inset-x-0 z-30 bg-slate-900/80 backdrop-blur-lg border-b border-slate-700/50">
      <div className="flex items-center px-5 py-3">
        <span className="text-base font-bold text-white tracking-wide">HOLE FINDER</span>
        <div className="flex-1" />
        <Link
          to="/playground"
          className="flex items-center gap-2 text-sm text-slate-400 hover:text-white transition-colors"
        >
          <Settings2 size={16} />
          Advanced
        </Link>
      </div>
    </div>
  );
}
