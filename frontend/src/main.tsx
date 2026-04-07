import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App.tsx'

// Suppress MapLibre terrain DOMExceptions (known Firefox bug, unfixed upstream).
// MapLibre's _loadTile throws async DOMExceptions during terrain tile decoding
// via createImageBitmap. These are cosmetic — terrain recovers after retry.
// Three layers of suppression because Firefox is stubborn about logging these:
// 1. window error handler → preventDefault stops "Uncaught" badge
// 2. unhandledrejection → catches async promise rejections
// 3. console.error monkeypatch → prevents Firefox from logging them at all
// Refs: maplibre/maplibre-gl-js#3537, #1551, #3982
const _origConsoleError = console.error;
console.error = (...args: unknown[]) => {
  const first = args[0];
  if (first instanceof DOMException && first.message?.includes('usable')) return;
  if (first instanceof Error && first.message?.includes('Invalid LngLat')) return;
  _origConsoleError.apply(console, args);
};
window.addEventListener('error', (e) => {
  if (e.error instanceof DOMException && e.error.message?.includes('usable')) { e.preventDefault(); return; }
  if (e.error?.message?.includes('Invalid LngLat')) { e.preventDefault(); return; }
});
window.addEventListener('unhandledrejection', (e) => {
  if (e.reason instanceof DOMException && e.reason.message?.includes('usable')) { e.preventDefault(); return; }
  if (e.reason?.message?.includes('Invalid LngLat')) { e.preventDefault(); return; }
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
)
