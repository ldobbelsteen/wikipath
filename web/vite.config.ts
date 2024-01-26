import { defineConfig } from "vite";
import eslint from "vite-plugin-eslint";

export default defineConfig({
  plugins: [eslint()],
  server: {
    proxy: {
      "/api": "http://localhost:1789",
    },
  },
});
