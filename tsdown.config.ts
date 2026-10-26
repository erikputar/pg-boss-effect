import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts"],
  format: "esm",
  platform: "node",
  dts: true,
  fixedExtension: false,
  sourcemap: true,
  clean: true,
  deps: {
    neverBundle: ["effect", "pg-boss"],
  },
});
