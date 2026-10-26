import { defineConfig } from "vitest/config";

import { fileURLToPath } from "node:url";

export default defineConfig({
  test: {
    passWithNoTests: true,
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["src/**/*.unit.ts"],
        },
      },
      {
        extends: true,
        test: {
          name: "integration",
          include: ["src/**/*.integration.ts"],
          globalSetup: fileURLToPath(
            new URL("./vitest.integration.globalSetup.ts", import.meta.url),
          ),
          env: {
            PGBOSS_TEST_DATABASE_URL:
              "postgresql://pgboss:pgboss@localhost:5435/pgboss",
          },
        },
      },
    ],
  },
});
