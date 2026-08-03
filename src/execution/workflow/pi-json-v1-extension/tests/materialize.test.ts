import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { materializePiJsonV1Extension } from "../src/materialize.ts";
import type { PiJsonV1ExtensionConfig } from "../src/pi-json-v1-extension.ts";

const templateUrl = new URL("../src/pi-json-v1-extension.ts", import.meta.url);
const fixedConfig: PiJsonV1ExtensionConfig = {
  toolName: "scherzo_result_unit_fixed",
  socketPath: "/tmp/scherzo/unit-fixed/result.sock",
  parameters: {
    type: "object",
    properties: {
      result: { type: "string" },
    },
    required: ["result"],
    additionalProperties: false,
  },
};

test("materialization is byte-identical for fixed inputs", async () => {
  const template = await readFile(templateUrl, "utf8");

  const first = materializePiJsonV1Extension(template, fixedConfig);
  const second = materializePiJsonV1Extension(template, fixedConfig);

  assert.equal(first, second);
  assert.ok(first.includes(JSON.stringify(JSON.stringify(fixedConfig))));
  assert.ok(!first.includes("__SCHERZO_PI_JSON_V1_CONFIG_JSON__"));
});

test("materialization rejects a template without exactly one marker", () => {
  assert.throws(() =>
    materializePiJsonV1Extension("export {};\n", fixedConfig),
  );
});

test("the representative resource id matches the retained schema bytes", async () => {
  const [schemaBytes, inputBytes] = await Promise.all([
    readFile(
      new URL("../fixtures/workflow-result.schema.json", import.meta.url),
    ),
    readFile(
      new URL("../fixtures/materialization-input.json", import.meta.url),
      "utf8",
    ),
  ]);
  const input = JSON.parse(inputBytes) as {
    parameters: {
      $defs: { workflowResult: { $id: string } };
      properties: { result: { $ref: string } };
    };
  };
  const digest = createHash("sha256").update(schemaBytes).digest("hex");
  const resourceId = `https://schemas.scherzo.invalid/workflow-result/${digest}`;

  assert.equal(input.parameters.$defs.workflowResult.$id, resourceId);
  assert.equal(input.parameters.properties.result.$ref, resourceId);
});
