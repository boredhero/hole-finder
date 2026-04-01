import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App.tsx'

// Suppress MapLibre terrain DOMExceptions (known Firefox bug, unfixed upstream).
// MapLibre's _loadTile throws async DOMExceptions during terrain tile decoding
// via createImageBitmap. These propagate through react-map-gl's _onEvent wrapper
// and cannot be caught by try/catch, map.on('error'), or rAF delays.
// The errors are cosmetic — terrain recovers and renders correctly after retry.
// Refs: maplibre/maplibre-gl-js#3537, #1551, #3982
window.addEventListener('error', (e) => {
  if (e.error instanceof DOMException && e.error.message?.includes('usable')) {
    e.preventDefault();
  }
});
window.addEventListener('unhandledrejection', (e) => {
  if (e.reason instanceof DOMException && e.reason.message?.includes('usable')) {
    e.preventDefault();
  }
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
)
