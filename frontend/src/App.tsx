import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Routes, Route } from 'react-router-dom';
import LandingPage from './pages/LandingPage';
import PlaygroundPage from './pages/PlaygroundPage';

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
      <Routes>
        <Route path="/" element={<LandingPage />} />
        <Route path="/playground" element={<PlaygroundPage />} />
      </Routes>
    </QueryClientProvider>
  );
}
