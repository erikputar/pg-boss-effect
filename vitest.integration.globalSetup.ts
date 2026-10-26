import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const composeFile = fileURLToPath(
  new URL("./compose.test.yml", import.meta.url),
);

export async function setup() {
  execSync(`docker compose -f ${composeFile} up -d --wait`, {
    stdio: "inherit",
  });
}

export async function teardown() {
  execSync(`docker compose -f ${composeFile} down -v`, {
    stdio: "inherit",
  });
}
