import { useState, useCallback } from 'react';
import { Link } from 'react-router-dom';
import MapView from '../components/Map/MapView';
import TopBar from '../components/Explore/TopBar';
import BottomDrawer from '../components/Explore/BottomDrawer';
import SearchButton from '../components/Explore/SearchButton';
import ProcessingScreen from '../components/Explore/ProcessingScreen';
import ResultsSplash from '../components/Explore/ResultsSplash';
import SwipeCard from '../components/Explore/SwipeCard';
import { useRegions } from '../hooks/useRegions';
import { useJobProgress } from '../hooks/useJobProgress';
import { useStore } from '../store';
import { geocodeZip, getDetectionCount, getDetections, startConsumerScan } from '../api/client';
import { geometryToBbox, formatRegionName } from '../utils';
import { Locate, Map as MapIcon, ArrowLeft, Settings2, Loader2, AlertCircle } from 'lucide-react';
import type { Detection } from '../types';

type Phase = 'splash' | 'regionPicker' | 'processing' | 'results' | 'tour' | 'explore';

export default function LandingPage() {
  const [phase, setPhase] = useState<Phase>('splash');
  const [geoLoading, setGeoLoading] = useState(false);
  const [geoError, setGeoError] = useState<string | null>(null);
  const [zipCode, setZipCode] = useState('');
  const [zipLoading, setZipLoading] = useState(false);
  const [zipError, setZipError] = useState<string | null>(null);
  const [swipeDirection, setSwipeDirection] = useState(1);

  const setTargetViewState = useStore((s) => s.setTargetViewState);
  const setUserLocation = useStore((s) => s.setUserLocation);
  const setSearchStale = useStore((s) => s.setSearchStale);
  const userLocation = useStore((s) => s.userLocation);

  const activeJobId = useStore((s) => s.activeJobId);
  const setActiveJobId = useStore((s) => s.setActiveJobId);
  const tourDetections = useStore((s) => s.tourDetections);
  const setTourDetections = useStore((s) => s.setTourDetections);
  const tourIndex = useStore((s) => s.tourIndex);
  const setTourIndex = useStore((s) => s.setTourIndex);

  const jobProgress = useJobProgress(phase === 'processing' ? activeJobId : null);

  // Shared flow after getting a location (from geo or zip)
  const handleLocationAcquired = useCallback(async (lat: number, lon: number) => {
    setUserLocation({ lat, lon });
    setTargetViewState({ longitude: lon, latitude: lat, zoom: 14, pitch: 45, bearing: -15 });

    try {
      const { count } = await getDetectionCount(lat, lon, 3);

      if (count > 0) {
        // Detections exist — fetch top 50 for tour
        const r = 3 / 111.32;
        const data = await getDetections({
          west: lon - r, south: lat - r, east: lon + r, north: lat + r,
          min_confidence: 0.5,
          limit: 50,
        });
        const dets: Detection[] = (data.features || []).map((f: any) => ({
          id: f.id,
          lon: f.geometry.coordinates[0],
          lat: f.geometry.coordinates[1],
          ...f.properties,
        }));
        setTourDetections(dets);
        setTourIndex(0);
        setPhase('results');
      } else {
        // No detections — start processing
        const { job_id } = await startConsumerScan(lat, lon, 3);
        setActiveJobId(job_id);
        setPhase('processing');
      }
    } catch {
      // If count check fails, just go to explore
      setSearchStale(true);
      setPhase('explore');
    }
  }, [setUserLocation, setTargetViewState, setTourDetections, setTourIndex, setActiveJobId, setSearchStale]);

  const handleFindNearMe = useCallback(() => {
    if (!navigator.geolocation) {
      setGeoError("Your browser doesn't support geolocation.");
      return;
    }
    setGeoLoading(true);
    setGeoError(null);
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        setGeoLoading(false);
        handleLocationAcquired(pos.coords.latitude, pos.coords.longitude);
      },
      (err) => {
        setGeoLoading(false);
        const messages: Record<number, string> = {
          1: "Location permission denied. Allow location access in your browser settings, or enter your zip code below.",
          2: "Location unavailable. Try entering your zip code instead.",
          3: "Location request timed out. Try entering your zip code instead.",
        };
        setGeoError(messages[err.code] ?? `Geolocation error (code ${err.code}): ${err.message}`);
      },
      { timeout: 10000 },
    );
  }, [handleLocationAcquired]);

  const handleZipSubmit = useCallback(async () => {
    if (!/^\d{5}$/.test(zipCode)) {
      setZipError('Enter a valid 5-digit zip code');
      return;
    }
    setZipLoading(true);
    setZipError(null);
    try {
      const { lat, lon } = await geocodeZip(zipCode);
      setZipLoading(false);
      handleLocationAcquired(lat, lon);
    } catch {
      setZipLoading(false);
      setZipError('Invalid or unrecognized zip code');
    }
  }, [zipCode, handleLocationAcquired]);

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

  // Handle processing completion
  const handleProcessingComplete = useCallback(async () => {
    if (!userLocation) {
      setPhase('explore');
      return;
    }
    // Fetch detections from the processed area
    const { lat, lon } = userLocation;
    const r = 3 / 111.32;
    try {
      const data = await getDetections({
        west: lon - r, south: lat - r, east: lon + r, north: lat + r,
        min_confidence: 0.5,
        limit: 50,
      });
      const dets: Detection[] = (data.features || []).map((f: any) => ({
        id: f.id,
        lon: f.geometry.coordinates[0],
        lat: f.geometry.coordinates[1],
        ...f.properties,
      }));
      setTourDetections(dets);
      setTourIndex(0);
      setActiveJobId(null);

      if (dets.length > 0) {
        setPhase('results');
      } else {
        setSearchStale(true);
        setPhase('explore');
      }
    } catch {
      setPhase('explore');
    }
  }, [userLocation, setTourDetections, setTourIndex, setActiveJobId, setSearchStale]);

  // Watch for job completion
  if (phase === 'processing' && jobProgress.status === 'COMPLETED') {
    handleProcessingComplete();
  }

  // Tour navigation
  const handleTourNext = useCallback(() => {
    if (tourIndex < tourDetections.length - 1) {
      const next = tourIndex + 1;
      setTourIndex(next);
      setSwipeDirection(1);
      const d = tourDetections[next];
      setTargetViewState({ longitude: d.lon, latitude: d.lat, zoom: 15, pitch: 45, bearing: -15 });
    }
  }, [tourIndex, tourDetections, setTourIndex, setTargetViewState]);

  const handleTourPrev = useCallback(() => {
    if (tourIndex > 0) {
      const prev = tourIndex - 1;
      setTourIndex(prev);
      setSwipeDirection(-1);
      const d = tourDetections[prev];
      setTargetViewState({ longitude: d.lon, latitude: d.lat, zoom: 15, pitch: 45, bearing: -15 });
    }
  }, [tourIndex, tourDetections, setTourIndex, setTargetViewState]);

  const handleTourExit = useCallback(() => {
    setSearchStale(true);
    setPhase('explore');
  }, [setSearchStale]);

  // --- RENDER ---

  // Tour phase: map + swipeable card
  if (phase === 'tour') {
    const d = tourDetections[tourIndex];
    return (
      <div className="relative h-full w-full">
        <div className="absolute inset-0">
          <MapView />
        </div>
        {d && (
          <SwipeCard
            detection={d}
            userLocation={userLocation}
            currentIndex={tourIndex}
            totalCount={tourDetections.length}
            direction={swipeDirection}
            onNext={handleTourNext}
            onPrev={handleTourPrev}
            onExit={handleTourExit}
          />
        )}
      </div>
    );
  }

  // Results splash (auto-transitions to tour)
  if (phase === 'results') {
    return (
      <div className="relative h-full w-full">
        <div className="absolute inset-0">
          <MapView />
        </div>
        <ResultsSplash
          detections={tourDetections}
          onDismiss={() => {
            if (tourDetections.length > 0) {
              // Fly to the best detection
              const best = tourDetections[0];
              setTargetViewState({ longitude: best.lon, latitude: best.lat, zoom: 15, pitch: 45, bearing: -15 });
              setPhase('tour');
            } else {
              setSearchStale(true);
              setPhase('explore');
            }
          }}
        />
      </div>
    );
  }

  // Processing phase: map loads underneath, processing screen on top
  if (phase === 'processing') {
    return (
      <div className="relative h-full w-full">
        <div className="absolute inset-0">
          <MapView />
        </div>
        <ProcessingScreen
          progress={jobProgress.progress}
          stage={jobProgress.stage}
          error={jobProgress.status === 'FAILED' ? (jobProgress.error || 'Processing failed') : null}
          onRetry={() => {
            setActiveJobId(null);
            setPhase('splash');
          }}
        />
      </div>
    );
  }

  // Explore phase
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

  // Region picker
  if (phase === 'regionPicker') {
    return <RegionPicker onPick={handlePickRegion} onBack={() => setPhase('splash')} />;
  }

  // Splash
  return (
    <div className="h-full w-full flex items-center justify-center bg-slate-950 relative">
      <Link
        to="/playground"
        className="absolute top-5 right-5 flex items-center gap-2 text-sm text-slate-500 hover:text-slate-300 transition-colors"
      >
        <Settings2 size={16} />
        Advanced Playground
      </Link>

      <div className="text-center px-8 max-w-xl">
        <h1 className="text-6xl md:text-7xl font-black text-white mb-3 tracking-tight">Hole Finder</h1>
        <p className="text-slate-400 text-lg md:text-xl mb-10 leading-relaxed">
          Discover caves, mines, sinkholes & more hidden in LiDAR terrain data
        </p>

        {/* Geo error */}
        {geoError && (
          <div className="flex items-center gap-3 bg-red-900/30 border border-red-700/50 text-red-300 text-base rounded px-5 py-4 mb-6">
            <AlertCircle size={18} className="flex-shrink-0" />
            {geoError}
          </div>
        )}

        {/* Buttons */}
        <div className="flex flex-col gap-4">
          <button
            onClick={handleFindNearMe}
            disabled={geoLoading}
            className="flex items-center justify-center gap-3 bg-blue-600 hover:bg-blue-500 disabled:opacity-60 text-white font-semibold py-4 px-8 rounded text-lg transition-colors"
          >
            {geoLoading ? <Loader2 size={22} className="animate-spin" /> : <Locate size={22} />}
            {geoLoading ? 'Getting location...' : 'Find a Hole Near Me'}
          </button>
          <button
            onClick={() => setPhase('regionPicker')}
            className="flex items-center justify-center gap-3 bg-slate-800 hover:bg-slate-700 text-slate-200 font-semibold py-4 px-8 rounded text-lg transition-colors"
          >
            <MapIcon size={22} />
            Pick a Region
          </button>

          {/* Zip code fallback — appears after geolocation error */}
          {geoError && (
            <div className="flex items-stretch gap-4">
              <input
                type="text"
                inputMode="numeric"
                maxLength={5}
                placeholder="Zip code"
                value={zipCode}
                onChange={(e) => { setZipCode(e.target.value.replace(/\D/g, '')); setZipError(null); }}
                onKeyDown={(e) => e.key === 'Enter' && handleZipSubmit()}
                className="flex-1 min-w-0 bg-slate-800 border-2 border-slate-600 text-white text-xl py-4 px-6 rounded focus:outline-none focus:border-blue-500 transition-colors placeholder:text-slate-500"
              />
              <button
                onClick={handleZipSubmit}
                disabled={zipLoading || zipCode.length !== 5}
                className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white font-semibold py-4 px-10 rounded text-lg transition-colors flex-shrink-0"
              >
                {zipLoading ? <Loader2 size={22} className="animate-spin" /> : 'Go'}
              </button>
            </div>
          )}
          {zipError && (
            <p className="text-red-400 text-base">{zipError}</p>
          )}
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
                className="text-left bg-slate-800/80 hover:bg-slate-700 border border-slate-700/50 hover:border-slate-600 rounded p-5 transition-all"
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
