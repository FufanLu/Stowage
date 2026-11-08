import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies the API to a local stowd admin listener
// (STOWD=127.0.0.1:9481 by default).
const stowd = process.env.STOWD ?? "127.0.0.1:9481";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": `http://${stowd}`,
      "/status": `http://${stowd}`,
      "/metrics": `http://${stowd}`,
    },
  },
});
