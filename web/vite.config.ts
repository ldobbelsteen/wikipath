import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import pkg from "./package.json";

export default defineConfig({
  plugins: [tailwindcss()],
  define: {
    "import.meta.env.VERSION": JSON.stringify(pkg.version),
  },
  server: {
    proxy: {
      "/api": "http://localhost:1789",
    },
  },
});
