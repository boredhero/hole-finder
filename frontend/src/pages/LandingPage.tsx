import { useState, useCallback } from 'react';
import { Link } from 'react-router-dom';
import MapView from '../components/Map/MapView';
import TopBar from '../components/Explore/TopBar';
import BottomDrawer from '../components/Explore/BottomDrawer';
import { useRegions } from '../hooks/useRegions';
import { useStore } from '../store';
import { getUserBbox, geometryToBbox, formatRegionName } from '../utils';
import { Locate, Map as MapIcon, ArrowLeft, Settings2, Loader2 } from 'lucide-react';

type Phase = 'splash' | 'regionPicker' | 'explore';

export default function LandingPage() {
  const [phase, setPhase] = useState<Phase>('splash');
  const [geoLoading, setGeoLoading] = useState(false);

  const setBbox = useStore((s) => s.setBbox);
  const setTargetViewState = useStore((s) => s.setTargetViewState);
  const setUserLocation = useStore((s) => s.setUserLocation);

  const handleFindNearMe = useCallback(() => {
    if (!navigator.geolocation) {
      setPhase('regionPicker');
      return;
    }
    setGeoLoading(true);
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const { latitude: lat, longitude: lon } = pos.coords;
        setUserLocation({ lat, lon });
        const bbox = getUserBbox(lat, lon, 100);
        setBbox(bbox);
        setTargetViewState({ longitude: lon, latitude: lat, zoom: 10, pitch: 45, bearing: -15 });
        setGeoLoading(false);
        setPhase('explore');
      },
      () => {
        setGeoLoading(false);
        setPhase('regionPicker');
      },
      { timeout: 10000 },
    );
  }, [setBbox, setTargetViewState, setUserLocation]);

  const handlePickRegion = useCallback((geometry: any) => {
    const bbox = geometryToBbox(geometry);
    setBbox(bbox);
    const centerLon = (bbox[0] + bbox[2]) / 2;
    const centerLat = (bbox[1] + bbox[3]) / 2;
    const lonSpan = bbox[2] - bbox[0];
    const zoom = lonSpan > 3 ? 7 : lonSpan > 1 ? 9 : 11;
    setTargetViewState({ longitude: centerLon, latitude: centerLat, zoom, pitch: 45, bearing: -15 });
    setPhase('explore');
  }, [setBbox, setTargetViewState]);

  if (phase === 'explore') {
    return (
      <div className="relative h-full w-full">
        <div className="absolute inset-0">
          <MapView />
        </div>
        <TopBar />
        <BottomDrawer />
      </div>
    );
  }

  if (phase === 'regionPicker') {
    return <RegionPicker onPick={handlePickRegion} onBack={() => setPhase('splash')} />;
  }

  // Splash
  return (
    <div className="h-full w-full flex items-center justify-center bg-slate-950 relative">
      {/* Playground link */}
      <Link
        to="/playground"
        className="absolute top-4 right-4 flex items-center gap-1.5 text-xs text-slate-500 hover:text-slate-300 transition-colors"
      >
        <Settings2 size={13} />
        Advanced Playground
      </Link>

      <div className="text-center px-6 max-w-md">
        {/* Title */}
        <h1 className="text-5xl font-black text-white mb-2 tracking-tight">Hole Finder</h1>
        <p className="text-slate-400 text-base mb-8">
          Discover caves, mines, sinkholes & more hidden in LiDAR terrain data
        </p>

        {/* Buttons */}
        <div className="flex flex-col gap-3">
          <button
            onClick={handleFindNearMe}
            disabled={geoLoading}
            className="flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-500 disabled:opacity-60 text-white font-medium py-3 px-6 rounded-xl text-sm transition-colors"
          >
            {geoLoading ? <Loader2 size={16} className="animate-spin" /> : <Locate size={16} />}
            {geoLoading ? 'Getting location...' : 'Find Near Me'}
          </button>
          <button
            onClick={() => setPhase('regionPicker')}
            className="flex items-center justify-center gap-2 bg-slate-800 hover:bg-slate-700 text-slate-200 font-medium py-3 px-6 rounded-xl text-sm transition-colors"
          >
            <MapIcon size={16} />
            Pick a Region
          </button>
        </div>

        <p className="text-xs text-slate-600 mt-8">
          9 states &middot; 13 regions &middot; 36 validation sites
        </p>
      </div>
    </div>
  );
}

function RegionPicker({ onPick, onBack }: { onPick: (geometry: any) => void; onBack: () => void }) {
  const { data: regions = [], isLoading } = useRegions();

  return (
    <div className="h-full w-full bg-slate-950 overflow-y-auto">
      <div className="max-w-3xl mx-auto px-6 py-8">
        {/* Header */}
        <div className="flex items-center gap-3 mb-6">
          <button onClick={onBack} className="text-slate-400 hover:text-white transition-colors">
            <ArrowLeft size={20} />
          </button>
          <h2 className="text-xl font-bold text-white">Pick a Region</h2>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center py-20">
            <Loader2 size={24} className="animate-spin text-slate-500" />
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {regions.map((region) => (
              <button
                key={region.name}
                onClick={() => onPick(region.geometry)}
                className="text-left bg-slate-800/80 hover:bg-slate-700 border border-slate-700/50 hover:border-slate-600 rounded-xl p-4 transition-all"
              >
                <h3 className="text-sm font-semibold text-white mb-1">
                  {formatRegionName(region.name)}
                </h3>
                {region.description && (
                  <p className="text-xs text-slate-400 leading-relaxed">{region.description}</p>
                )}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
