import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: { environment: "node", include: ["*.test.ts"] },
  resolve: {
    alias: {
      "@earendil-works/pi-coding-agent": resolve(__dirname, "test/mocks/pi-coding-agent.ts"),
      typebox: resolve(__dirname, "test/mocks/typebox.ts"),
    },
  },
});
