import { mkdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { materializePiJsonV1Extension } from "../src/materialize.ts";
import type { PiJsonV1ExtensionConfig } from "../src/pi-json-v1-extension.ts";

const packageRoot = fileURLToPath(new URL("../", import.meta.url));
const templatePath = fileURLToPath(
  new URL("src/pi-json-v1-extension.ts", new URL("../", import.meta.url)),
);
const inputPath = fileURLToPath(
  new URL(
    "fixtures/materialization-input.json",
    new URL("../", import.meta.url),
  ),
);
const fixtureDirectory = fileURLToPath(
  new URL("fixtures/generated/", new URL("../", import.meta.url)),
);
const fixturePath = fileURLToPath(
  new URL(
    "fixtures/generated/pi-json-v1-extension.ts",
    new URL("../", import.meta.url),
  ),
);

async function materializeFixture(): Promise<string> {
  const [template, input] = await Promise.all([
    readFile(templatePath, "utf8"),
    readFile(inputPath, "utf8"),
  ]);
  const config = JSON.parse(input) as PiJsonV1ExtensionConfig;
  return materializePiJsonV1Extension(template, config);
}

async function main(): Promise<void> {
  const argumentsValue = process.argv.slice(2);
  const check = argumentsValue.length === 1 && argumentsValue[0] === "--check";
  if (!check && argumentsValue.length !== 0) {
    throw new Error("usage: materialize-fixture.ts [--check]");
  }

  const materialized = await materializeFixture();
  if (!check) {
    await mkdir(fixtureDirectory, { recursive: true });
    await writeFile(fixturePath, materialized, "utf8");
    return;
  }

  const fixture = await readFile(fixturePath, "utf8");
  if (fixture !== materialized) {
    throw new Error(
      `Generated PiJsonV1 fixture is stale; run npm run fixture:generate from ${packageRoot}`,
    );
  }
}

await main();
