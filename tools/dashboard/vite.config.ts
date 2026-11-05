import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The build is committed to the Python package (so `pip install arti` needs no Node), so keep the
// output small and the file names stable: a rebuild of unchanged sources produces no diff.
export default defineConfig({
  base: "./",
  build: {
    outDir: "../../src/arti/dashboard/static",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "assets/dashboard.js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: "assets/[name][extname]",
      },
    },
  },
  plugins: [react()],
  server: {
    // `npm run dev` serves the page with hot reloading; run `arti dashboard` alongside for the API.
    proxy: { "/api": "http://127.0.0.1:8000" },
  },
});
