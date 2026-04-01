import { useState, useCallback, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import MapView from '../components/Map/MapView';
import TopBar from '../components/Explore/TopBar';
import SettingsPanel from '../components/Explore/SettingsPanel';
import ProcessingScreen from '../components/Explore/ProcessingScreen';
import ResultsSplash from '../components/Explore/ResultsSplash';
import SwipeCard from '../components/Explore/SwipeCard';
import { useRegions } from '../hooks/useRegions';
import { useJobProgress } from '../hooks/useJobProgress';
import { useStore } from '../store';
import { geocodeZip, getDetections, startConsumerScan, warmTerrainCache } from '../api/client';
import { geometryToBbox, formatRegionName } from '../utils';
import { Locate, Map as MapIcon, Search, ArrowLeft, Settings2, Loader2, AlertCircle } from 'lucide-react';
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

  // Store the scan center so handleProcessingComplete doesn't depend on userLocation timing
  const scanCenter = useRef<{ lat: number; lon: number } | null>(null);
  const jobProgress = useJobProgress(phase === 'processing' ? activeJobId : null);

  // Shared flow after getting a location (from geo or zip)
  const handleLocationAcquired = useCallback(async (lat: number, lon: number) => {
    console.log('[HoleFinder] Location acquired:', lat, lon);
    scanCenter.current = { lat, lon };
    setUserLocation({ lat, lon });
    setTargetViewState({ longitude: lon, latitude: lat, zoom: 14, pitch: 45, bearing: -15 });

    try {
      const { job_id } = await startConsumerScan(lat, lon, 5);
      console.log('[HoleFinder] Scan started, job:', job_id);
      setActiveJobId(job_id);
      setPhase('processing');
    } catch (err) {
      console.error('[HoleFinder] Scan failed:', err);
      setSearchStale(true);
      setPhase('explore');
    }
  }, [setUserLocation, setTargetViewState, setActiveJobId, setSearchStale]);

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

  // Watch for job completion — fetch detections and transition to results/tour
  const completionHandled = useRef(false);
  useEffect(() => {
    if (phase !== 'processing') {
      completionHandled.current = false;
      return;
    }
    if (completionHandled.current) return;

    const status = jobProgress.status;
    if (status !== 'COMPLETED' && status !== 'FAILED') return;

    completionHandled.current = true;
    console.log('[HoleFinder] Job finished:', status);

    if (status === 'FAILED') {
      // ProcessingScreen already shows error UI
      return;
    }

    // Fetch detections from the scanned area
    const center = scanCenter.current;
    if (!center) {
      console.error('[HoleFinder] No scan center — going to explore');
      setSearchStale(true);
      setPhase('explore');
      return;
    }

    const r = 5 / 111.32;
    const west = center.lon - r, south = center.lat - r;
    const east = center.lon + r, north = center.lat + r;

    // Warm terrain cache BEFORE loading the map — prevents DOMExceptions
    // from uncached terrain tiles. Runs in parallel with detection fetch.
    const warmPromise = warmTerrainCache(west, south, east, north)
      .then((res) => console.log('[HoleFinder] Terrain cache warmed:', res))
      .catch((err) => console.warn('[HoleFinder] Terrain warm failed (non-fatal):', err));

    const detectPromise = getDetections({
      west, south, east, north,
      min_confidence: 0.5,
      limit: 50,
    });

    Promise.all([warmPromise, detectPromise]).then(([, data]) => {
      const dets: Detection[] = (data.features || []).map((f: any) => ({
        id: f.id,
        lon: f.geometry.coordinates[0],
        lat: f.geometry.coordinates[1],
        ...f.properties,
      }));
      console.log('[HoleFinder] Fetched', dets.length, 'detections for tour');
      setTourDetections(dets);
      setTourIndex(0);
      setActiveJobId(null);

      if (dets.length > 0) {
        setPhase('results');
      } else {
        setSearchStale(true);
        setPhase('explore');
      }
    }).catch((err) => {
      console.error('[HoleFinder] Failed to fetch detections:', err);
      setSearchStale(true);
      setPhase('explore');
    });
  }, [phase, jobProgress.status, setTourDetections, setTourIndex, setActiveJobId, setSearchStale, setTargetViewState]);

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
    // Keep tour data — swiper stays in explore phase. User can dismiss it there.
    setSearchStale(true);
    setPhase('explore');
  }, [setSearchStale]);

  // Dismiss swiper in explore (hides cards but doesn't clear data for re-show on search)
  const handleSwiperDismiss = useCallback(() => {
    setTourDetections([]);
    setTourIndex(0);
  }, [setTourDetections, setTourIndex]);

  // --- RENDER ---

  // Region picker (no map needed)
  if (phase === 'regionPicker') {
    return <RegionPicker onPick={handlePickRegion} onBack={() => setPhase('splash')} />;
  }

  // Map phases: processing, results, tour, explore
  // MapView stays mounted across ALL of these to keep the WebGL context alive
  // and prevent DOMExceptions from stale tile-load callbacks on context destruction.
  const isMapPhase = phase === 'processing' || phase === 'results' || phase === 'tour' || phase === 'explore';
  if (isMapPhase) {
    const tourDetection = tourDetections[tourIndex];
    return (
      <div className="relative h-full w-full">
        <div className="absolute inset-0">
          <MapView />
        </div>
        {/* Processing: opaque overlay hides map while GL context initializes behind it */}
        {phase === 'processing' && (
          <div className="absolute inset-0 z-20 bg-slate-950">
            <ProcessingScreen
              progress={jobProgress.progress}
              stage={jobProgress.stage}
              source={jobProgress.source}
              error={jobProgress.status === 'FAILED' ? (jobProgress.error || 'Processing failed') : null}
              onRetry={() => {
                setActiveJobId(null);
                setPhase('splash');
              }}
            />
          </div>
        )}
        {phase === 'results' && (
          <ResultsSplash
            detections={tourDetections}
            onDismiss={() => {
              if (tourDetections.length > 0) {
                const best = tourDetections[0];
                setTargetViewState({ longitude: best.lon, latitude: best.lat, zoom: 15, pitch: 45, bearing: -15 });
                setPhase('tour');
              } else {
                setSearchStale(true);
                setPhase('explore');
              }
            }}
          />
        )}
        {(phase === 'tour' || phase === 'explore') && (
          <>
            <TopBar />
            <SettingsPanel />
          </>
        )}
        {phase === 'tour' && tourDetection && (
          <SwipeCard
            detection={tourDetection}
            userLocation={userLocation}
            currentIndex={tourIndex}
            totalCount={tourDetections.length}
            direction={swipeDirection}
            onNext={handleTourNext}
            onPrev={handleTourPrev}
            onExit={handleTourExit}
          />
        )}
        {phase === 'explore' && (
          <>
            <ExploreSearchButton
              onResults={(dets) => {
                setTourDetections(dets);
                setTourIndex(0);
                if (dets.length > 0) {
                  const best = dets[0];
                  setTargetViewState({ longitude: best.lon, latitude: best.lat, zoom: 15, pitch: 45, bearing: -15 });
                }
              }}
            />
            {tourDetection && (
              <SwipeCard
                detection={tourDetection}
                userLocation={userLocation}
                currentIndex={tourIndex}
                totalCount={tourDetections.length}
                direction={swipeDirection}
                onNext={handleTourNext}
                onPrev={handleTourPrev}
                onExit={handleSwiperDismiss}
              />
            )}
          </>
        )}
      </div>
    );
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
            className="flex items-center justify-center gap-3 bg-cherry-500 hover:bg-cherry-400 disabled:opacity-60 text-white font-semibold py-4 px-8 rounded text-lg transition-colors"
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
                className="flex-1 min-w-0 bg-slate-800 border-2 border-slate-600 text-white text-xl py-4 px-6 rounded focus:outline-none focus:border-cherry-500 transition-colors placeholder:text-slate-500"
              />
              <button
                onClick={handleZipSubmit}
                disabled={zipLoading || zipCode.length !== 5}
                className="bg-cherry-500 hover:bg-cherry-400 disabled:opacity-50 text-white font-semibold py-4 px-10 rounded text-lg transition-colors flex-shrink-0"
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

/** Floating search button that fetches detections into the swiper */
function ExploreSearchButton({ onResults }: { onResults: (dets: Detection[]) => void }) {
  const searchStale = useStore((s) => s.searchStale);
  const bbox = useStore((s) => s.bbox);
  const setSearchStale = useStore((s) => s.setSearchStale);
  const [loading, setLoading] = useState(false);

  if (!searchStale || !bbox) return null;

  const handleSearch = async () => {
    setLoading(true);
    setSearchStale(false);
    try {
      const data = await getDetections({
        west: bbox[0], south: bbox[1], east: bbox[2], north: bbox[3],
        min_confidence: 0.5,
        limit: 50,
      });
      const dets: Detection[] = (data.features || []).map((f: any) => ({
        id: f.id,
        lon: f.geometry.coordinates[0],
        lat: f.geometry.coordinates[1],
        ...f.properties,
      }));
      console.log('[HoleFinder] Search found', dets.length, 'detections');
      onResults(dets);
    } catch (err) {
      console.error('[HoleFinder] Search failed:', err);
    }
    setLoading(false);
  };

  return (
    <button
      onClick={handleSearch}
      disabled={loading}
      className="fixed top-16 left-1/2 -translate-x-1/2 z-30 bg-cherry-500 hover:bg-cherry-400 disabled:opacity-60 text-white font-medium text-sm px-6 py-3 rounded shadow-lg flex items-center gap-2 transition-all"
    >
      {loading ? <Loader2 size={16} className="animate-spin" /> : <Search size={16} />}
      Search this area
    </button>
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
