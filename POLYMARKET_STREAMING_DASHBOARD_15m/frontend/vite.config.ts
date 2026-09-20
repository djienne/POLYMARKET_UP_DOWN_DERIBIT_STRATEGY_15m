import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5175,
    strictPort: true,
    host: "127.0.0.1",
    proxy: {
      "/api": "http://127.0.0.1:8798",
      "/ws": { target: "ws://127.0.0.1:8798", ws: true },
    },
  },
  preview: { port: 5175, strictPort: true, host: "127.0.0.1" },
});
