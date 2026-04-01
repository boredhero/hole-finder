import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronLeft, ChevronRight, ExternalLink, MapPin, X } from 'lucide-react';
import type { Detection } from '../../types';
import { FEATURE_COLORS, FEATURE_LABELS } from '../../types';
import { haversineDistance } from '../../utils';

interface SwipeCardProps {
  detection: Detection;
  userLocation: { lat: number; lon: number } | null;
  currentIndex: number;
  totalCount: number;
  direction: number;  // 1 = from right, -1 = from left
  onNext: () => void;
  onPrev: () => void;
  onExit: () => void;
}

const variants = {
  enter: (direction: number) => ({
    x: direction > 0 ? 300 : -300,
    opacity: 0,
  }),
  center: {
    x: 0,
    opacity: 1,
  },
  exit: (direction: number) => ({
    x: direction > 0 ? -300 : 300,
    opacity: 0,
  }),
};

export default function SwipeCard({
  detection: d,
  userLocation,
  currentIndex,
  totalCount,
  direction,
  onNext,
  onPrev,
  onExit,
}: SwipeCardProps) {
  const [dragX, setDragX] = useState(0);
  const color = FEATURE_COLORS[d.feature_type] || '#6b7280';
  const label = FEATURE_LABELS[d.feature_type] || 'Unknown';
  const pct = Math.round(d.confidence * 100);
  const distance = userLocation ? haversineDistance(userLocation.lat, userLocation.lon, d.lat, d.lon) : null;
  const mapsUrl = `https://www.google.com/maps?q=${d.lat},${d.lon}`;

  return (
    <div className="fixed bottom-0 inset-x-0 z-40 pb-6 px-4">
      {/* Navigation arrows (desktop) */}
      <div className="flex items-end justify-center gap-3 mb-3">
        <button
          onClick={onPrev}
          disabled={currentIndex === 0}
          className="hidden md:flex w-10 h-10 items-center justify-center bg-slate-800/90 hover:bg-slate-700 disabled:opacity-30 text-white rounded transition-colors"
        >
          <ChevronLeft size={20} />
        </button>

        {/* Swipeable card */}
        <AnimatePresence mode="wait" custom={direction}>
          <motion.div
            key={d.id}
            custom={direction}
            variants={variants}
            initial="enter"
            animate="center"
            exit="exit"
            transition={{ type: 'spring', stiffness: 300, damping: 30 }}
            drag="x"
            dragConstraints={{ left: 0, right: 0 }}
            dragElastic={0.7}
            onDrag={(_, info) => setDragX(info.offset.x)}
            onDragEnd={(_, info) => {
              setDragX(0);
              if (info.offset.x > 100 || info.velocity.x > 500) {
                if (currentIndex > 0) onPrev();
              } else if (info.offset.x < -100 || info.velocity.x < -500) {
                if (currentIndex < totalCount - 1) onNext();
              }
            }}
            style={{
              rotate: dragX * 0.03,
              opacity: 1 - Math.abs(dragX) * 0.002,
            }}
            className="w-full max-w-sm bg-slate-900/95 backdrop-blur-lg border border-slate-700 rounded p-5 cursor-grab active:cursor-grabbing touch-pan-y"
          >
            {/* Header */}
            <div className="flex items-center gap-2 mb-3">
              <span className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
              <span className="text-base font-semibold text-white">{label}</span>
              <span className="ml-auto text-sm font-mono text-slate-300">{pct}%</span>
            </div>

            {/* Confidence bar */}
            <div className="h-1.5 bg-slate-700 rounded overflow-hidden mb-4">
              <div className="h-full rounded" style={{ width: `${pct}%`, backgroundColor: color }} />
            </div>

            {/* Stats */}
            <div className="grid grid-cols-2 gap-2 text-xs mb-4">
              {d.depth_m != null && (
                <div className="bg-slate-800 rounded p-2">
                  <div className="text-slate-500 uppercase">Depth</div>
                  <div className="text-slate-100 font-mono">{d.depth_m.toFixed(1)} m <span className="text-slate-400">({(d.depth_m * 3.281).toFixed(1)} ft)</span></div>
                </div>
              )}
              {d.area_m2 != null && (
                <div className="bg-slate-800 rounded p-2">
                  <div className="text-slate-500 uppercase">Area</div>
                  <div className="text-slate-100 font-mono">{d.area_m2.toFixed(0)} m{'\u00B2'} <span className="text-slate-400">({(d.area_m2 * 10.764).toFixed(0)} ft{'\u00B2'})</span></div>
                </div>
              )}
              {(d.morphometrics?.volume_m3 as number) > 0 && (
                <div className="bg-slate-800 rounded p-2">
                  <div className="text-slate-500 uppercase">Volume</div>
                  <div className="text-slate-100 font-mono">{(d.morphometrics!.volume_m3 as number).toFixed(0)} m{'\u00B3'} <span className="text-slate-400">({((d.morphometrics!.volume_m3 as number) * 35.315).toFixed(0)} ft{'\u00B3'})</span></div>
                </div>
              )}
              {d.circularity != null && (
                <div className="bg-slate-800 rounded p-2">
                  <div className="text-slate-500 uppercase">Circularity</div>
                  <div className="text-slate-100 font-mono">{d.circularity.toFixed(2)}</div>
                </div>
              )}
              {distance != null && (
                <div className="bg-slate-800 rounded p-2">
                  <div className="text-slate-500 uppercase">Distance</div>
                  <div className="text-slate-100 font-mono">{distance.toFixed(1)} mi</div>
                </div>
              )}
            </div>

            {/* Coordinates + Maps link */}
            <div className="flex items-center gap-2 text-xs mb-4">
              <MapPin size={12} className="text-slate-500" />
              <span className="text-slate-400 font-mono">{d.lat.toFixed(5)}, {d.lon.toFixed(5)}</span>
              <a
                href={mapsUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="ml-auto flex items-center gap-1 bg-cherry-500 hover:bg-cherry-400 text-white px-3 py-1.5 rounded transition-colors"
                onClick={(e) => e.stopPropagation()}
              >
                <ExternalLink size={11} />
                Maps
              </a>
            </div>

            {/* Counter + swipe hint */}
            <div className="flex items-center justify-between text-xs text-slate-500">
              <span>{currentIndex + 1} of {totalCount}</span>
              <span className="md:hidden">Swipe to browse</span>
            </div>
          </motion.div>
        </AnimatePresence>

        <button
          onClick={onNext}
          disabled={currentIndex >= totalCount - 1}
          className="hidden md:flex w-10 h-10 items-center justify-center bg-slate-800/90 hover:bg-slate-700 disabled:opacity-30 text-white rounded transition-colors"
        >
          <ChevronRight size={20} />
        </button>
      </div>

      {/* Exit tour */}
      <div className="text-center">
        <button
          onClick={onExit}
          className="inline-flex items-center gap-1.5 text-sm text-slate-400 hover:text-white transition-colors"
        >
          <X size={14} />
          Explore on your own
        </button>
      </div>
    </div>
  );
}
