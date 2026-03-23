import { useRef, useCallback, useEffect } from 'react';
import { useStore } from '../../store';
import { useDetections } from '../../hooks/useDetections';
import DetectionCard from './DetectionCard';
import { ChevronUp, ChevronDown, X } from 'lucide-react';

export default function BottomDrawer() {
  const { data: detections = [] } = useDetections();
  const drawerState = useStore((s) => s.drawerState);
  const setDrawerState = useStore((s) => s.setDrawerState);
  const selectedDetection = useStore((s) => s.selectedDetection);
  const setSelectedDetection = useStore((s) => s.setSelectedDetection);
  const userLocation = useStore((s) => s.userLocation);
  const setTargetViewState = useStore((s) => s.setTargetViewState);

  const scrollRef = useRef<HTMLDivElement>(null);
  const touchStartY = useRef<number>(0);

  // When a detection is selected on the map, scroll to it in collapsed mode
  useEffect(() => {
    if (selectedDetection && drawerState === 'collapsed' && scrollRef.current) {
      const idx = detections.findIndex((d) => d.id === selectedDetection.id);
      if (idx >= 0) {
        const card = scrollRef.current.children[idx] as HTMLElement;
        card?.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
      }
    }
  }, [selectedDetection, detections, drawerState]);

  const handleCardClick = useCallback((d: typeof detections[0]) => {
    setSelectedDetection(d);
    setTargetViewState({ longitude: d.lon, latitude: d.lat, zoom: 15, pitch: 45, bearing: -15 });
    if (drawerState === 'expanded') {
      setDrawerState('detail');
    }
  }, [setSelectedDetection, setTargetViewState, drawerState, setDrawerState]);

  // Touch swipe handling
  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    touchStartY.current = e.touches[0].clientY;
  }, []);

  const handleTouchEnd = useCallback((e: React.TouchEvent) => {
    const delta = touchStartY.current - e.changedTouches[0].clientY;
    if (delta > 50 && drawerState === 'collapsed') {
      setDrawerState('expanded');
    } else if (delta < -50 && drawerState === 'expanded') {
      setDrawerState('collapsed');
    }
  }, [drawerState, setDrawerState]);

  if (detections.length === 0) {
    return (
      <div className="fixed bottom-0 inset-x-0 z-30 bg-slate-900/95 backdrop-blur-lg border-t border-slate-700 rounded-t-2xl p-5 text-center text-base text-slate-400">
        No detections in this area. Try zooming out or picking a different region.
      </div>
    );
  }

  // Detail view — single card expanded
  if (drawerState === 'detail' && selectedDetection) {
    return (
      <div className="fixed bottom-0 inset-x-0 z-30 bg-slate-900/95 backdrop-blur-lg border-t border-slate-700 rounded-t-2xl max-h-[60vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 pt-4 pb-3">
          <button
            onClick={() => setDrawerState('collapsed')}
            className="text-slate-400 hover:text-white flex items-center gap-1.5 text-sm"
          >
            <ChevronDown size={16} /> Back
          </button>
          <button onClick={() => { setSelectedDetection(null); setDrawerState('collapsed'); }} className="text-slate-400 hover:text-white">
            <X size={20} />
          </button>
        </div>
        <div className="px-5 pb-5">
          <DetectionCard detection={selectedDetection} userLocation={userLocation} selected />
        </div>
      </div>
    );
  }

  // Expanded view — vertical list
  if (drawerState === 'expanded') {
    return (
      <div
        className="fixed bottom-0 inset-x-0 z-30 bg-slate-900/95 backdrop-blur-lg border-t border-slate-700 rounded-t-2xl transition-all duration-300"
        style={{ height: '60vh' }}
        onTouchStart={handleTouchStart}
        onTouchEnd={handleTouchEnd}
      >
        {/* Handle bar */}
        <div className="flex justify-center pt-2 pb-1">
          <div className="w-10 h-1 bg-slate-600 rounded-full" />
        </div>
        <div className="flex items-center justify-between px-5 pb-3">
          <span className="text-base font-semibold text-slate-300">{detections.length} detections</span>
          <button onClick={() => setDrawerState('collapsed')} className="text-slate-400 hover:text-white">
            <ChevronDown size={20} />
          </button>
        </div>
        <div className="overflow-y-auto px-5 pb-5 flex flex-col gap-3" style={{ maxHeight: 'calc(60vh - 70px)' }}>
          {detections.map((d) => (
            <DetectionCard
              key={d.id}
              detection={d}
              userLocation={userLocation}
              selected={selectedDetection?.id === d.id}
              onClick={() => handleCardClick(d)}
            />
          ))}
        </div>
      </div>
    );
  }

  // Collapsed view — horizontal card strip
  return (
    <div
      className="fixed bottom-0 inset-x-0 z-30 bg-slate-900/95 backdrop-blur-lg border-t border-slate-700 rounded-t-2xl transition-all duration-300"
      onTouchStart={handleTouchStart}
      onTouchEnd={handleTouchEnd}
    >
      {/* Handle bar */}
      <div className="flex justify-center pt-2 pb-1">
        <div className="w-10 h-1 bg-slate-600 rounded-full" />
      </div>
      <div className="flex items-center justify-between px-5 pb-2">
        <span className="text-sm font-semibold text-slate-400">{detections.length} detections</span>
        <button
          onClick={() => setDrawerState('expanded')}
          className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-1"
        >
          See all <ChevronUp size={14} />
        </button>
      </div>
      <div
        ref={scrollRef}
        className="flex gap-3 overflow-x-auto px-5 pb-5 snap-x snap-mandatory scrollbar-hide"
      >
        {detections.slice(0, 50).map((d) => (
          <DetectionCard
            key={d.id}
            detection={d}
            compact
            userLocation={userLocation}
            selected={selectedDetection?.id === d.id}
            onClick={() => handleCardClick(d)}
          />
        ))}
      </div>
    </div>
  );
}
