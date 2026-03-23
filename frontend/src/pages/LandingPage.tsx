import { useState, useCallback } from 'react';
import { Link } from 'react-router-dom';
import MapView from '../components/Map/MapView';
import TopBar from '../components/Explore/TopBar';
import BottomDrawer from '../components/Explore/BottomDrawer';
import SearchButton from '../components/Explore/SearchButton';
import { useRegions } from '../hooks/useRegions';
import { useStore } from '../store';
import { geometryToBbox, formatRegionName } from '../utils';
import { Locate, Map as MapIcon, ArrowLeft, Settings2, Loader2, AlertCircle } from 'lucide-react';

type Phase = 'splash' | 'regionPicker' | 'explore';

export default function LandingPage() {
  const [phase, setPhase] = useState<Phase>('splash');
  const [geoLoading, setGeoLoading] = useState(false);
  const [geoError, setGeoError] = useState<string | null>(null);

  const setTargetViewState = useStore((s) => s.setTargetViewState);
  const setUserLocation = useStore((s) => s.setUserLocation);
  const setSearchStale = useStore((s) => s.setSearchStale);

  const handleFindNearMe = useCallback(() => {
    if (!navigator.geolocation) {
      setGeoError("Your browser doesn't support geolocation.");
      return;
    }
    setGeoLoading(true);
    setGeoError(null);
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const { latitude: lat, longitude: lon } = pos.coords;
        setUserLocation({ lat, lon });
        setTargetViewState({ longitude: lon, latitude: lat, zoom: 14, pitch: 45, bearing: -15 });
        setSearchStale(true);
        setGeoLoading(false);
        setPhase('explore');
      },
      () => {
        setGeoLoading(false);
        setGeoError("Couldn't get your location. Try again or pick a region.");
      },
      { timeout: 10000 },
    );
  }, [setTargetViewState, setUserLocation, setSearchStale]);

  const handlePickRegion = useCallback((geometry: any) => {
    const bbox = geometryToBbox(geometry);
    const centerLon = (bbox[0] + bbox[2]) / 2;
    const centerLat = (bbox[1] + bbox[3]) / 2;
    const lonSpan = bbox[2] - bbox[0];
    const zoom = lonSpan > 3 ? 7 : lonSpan > 1 ? 9 : 11;
    setTargetViewState({ longitude: centerLon, latitude: centerLat, zoom, pitch: 45, bearing: -15 });
    setSearchStale(true);
    setPhase('explore');
  }, [setTargetViewState, setSearchStale]);

  if (phase === 'explore') {
    return (
      <div className="relative h-full w-full">
        <div className="absolute inset-0">
          <MapView />
        </div>
        <TopBar />
        <SearchButton />
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
        className="absolute top-5 right-5 flex items-center gap-2 text-sm text-slate-500 hover:text-slate-300 transition-colors"
      >
        <Settings2 size={16} />
        Advanced Playground
      </Link>

      <div className="text-center px-8 max-w-xl">
        {/* Title */}
        <h1 className="text-6xl md:text-7xl font-black text-white mb-3 tracking-tight">Hole Finder</h1>
        <p className="text-slate-400 text-lg md:text-xl mb-10 leading-relaxed">
          Discover caves, mines, sinkholes & more hidden in LiDAR terrain data
        </p>

        {/* Geo error */}
        {geoError && (
          <div className="flex items-center gap-2 bg-red-900/30 border border-red-700/50 text-red-300 text-sm rounded-xl px-4 py-3 mb-4">
            <AlertCircle size={16} className="flex-shrink-0" />
            {geoError}
          </div>
        )}

        {/* Buttons */}
        <div className="flex flex-col gap-4">
          <button
            onClick={handleFindNearMe}
            disabled={geoLoading}
            className="flex items-center justify-center gap-3 bg-blue-600 hover:bg-blue-500 disabled:opacity-60 text-white font-semibold py-4 px-8 rounded-2xl text-lg transition-colors"
          >
            {geoLoading ? <Loader2 size={22} className="animate-spin" /> : <Locate size={22} />}
            {geoLoading ? 'Getting location...' : 'Find a Hole Near Me'}
          </button>
          <button
            onClick={() => setPhase('regionPicker')}
            className="flex items-center justify-center gap-3 bg-slate-800 hover:bg-slate-700 text-slate-200 font-semibold py-4 px-8 rounded-2xl text-lg transition-colors"
          >
            <MapIcon size={22} />
            Pick a Region
          </button>
        </div>

        <p className="text-sm text-slate-600 mt-10">
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
      <div className="max-w-4xl mx-auto px-8 py-10">
        {/* Header */}
        <div className="flex items-center gap-4 mb-8">
          <button onClick={onBack} className="text-slate-400 hover:text-white transition-colors">
            <ArrowLeft size={24} />
          </button>
          <h2 className="text-2xl md:text-3xl font-bold text-white">Pick a Region</h2>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center py-20">
            <Loader2 size={28} className="animate-spin text-slate-500" />
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {regions.map((region) => (
              <button
                key={region.name}
                onClick={() => onPick(region.geometry)}
                className="text-left bg-slate-800/80 hover:bg-slate-700 border border-slate-700/50 hover:border-slate-600 rounded-2xl p-5 transition-all"
              >
                <h3 className="text-base font-semibold text-white mb-1.5">
                  {formatRegionName(region.name)}
                </h3>
                {region.description && (
                  <p className="text-sm text-slate-400 leading-relaxed">{region.description}</p>
                )}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
