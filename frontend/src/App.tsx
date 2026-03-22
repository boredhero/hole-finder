import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import MapView from './components/Map/MapView';
import Sidebar from './components/Sidebar/Sidebar';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="relative h-full w-full">
        {/* Map fills entire viewport */}
        <div className="absolute inset-0">
          <MapView />
        </div>

        {/* Sidebar overlays on left (desktop) or bottom (mobile) */}
        <Sidebar />
      </div>
    </QueryClientProvider>
  );
}
