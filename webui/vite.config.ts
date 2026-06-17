import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/replay': 'http://127.0.0.1:5800',
      '/trading': 'http://127.0.0.1:5800',
      '/market': 'http://127.0.0.1:5800'
    }
  }
});
