/** @type {import('vite').UserConfig} */
export default {
  server: {
    proxy: {
      "/api": "http://localhost:1789",
    },
  },
};
