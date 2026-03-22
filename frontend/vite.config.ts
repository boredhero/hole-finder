import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://192.168.1.111:8000',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://192.168.1.111:8000',
        ws: true,
      },
    },
  },
})
