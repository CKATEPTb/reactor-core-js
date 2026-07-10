import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const sourceRoot = fileURLToPath(new URL("./src", import.meta.url));
const sourceEntry = (path: string) => fileURLToPath(new URL(`./src/${path}`, import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@": sourceRoot
    }
  },
  build: {
    target: "es2020",
    outDir: "dist",
    emptyOutDir: true,
    minify: false,
    sourcemap: true,
    lib: {
      entry: {
        index: sourceEntry("index.ts"),
        "publisher/flux-entrypoint": sourceEntry("publisher/flux-entrypoint.ts"),
        "publisher/mono-entrypoint": sourceEntry("publisher/mono-entrypoint.ts"),
        "schedulers/index": sourceEntry("schedulers/index.ts"),
        "sinks/index": sourceEntry("sinks/index.ts"),
        "subscription/reactive-streams": sourceEntry("subscription/reactive-streams.ts")
      },
      formats: ["es"]
    },
    rollupOptions: {
      treeshake: false,
      output: {
        preserveModules: true,
        preserveModulesRoot: "src",
        entryFileNames: "[name].js",
        chunkFileNames: "chunks/[name]-[hash].js"
      }
    }
  }
});
