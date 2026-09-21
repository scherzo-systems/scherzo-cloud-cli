import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const packageRoot = new URL("../", import.meta.url);
const resultExtensionImports = new Map([
  ["@earendil-works/pi-coding-agent", "type"],
  ["node:net", "runtime"],
  ["typebox", "runtime"],
]);
const runtimeSources = [
  {
    path: new URL("src/pi-json-v1-extension.ts", packageRoot),
    expectedImports: resultExtensionImports,
  },
  {
    path: new URL("fixtures/generated/pi-json-v1-extension.ts", packageRoot),
    expectedImports: resultExtensionImports,
  },
  {
    path: new URL("src/pi-json-v1-input.ts", packageRoot),
    expectedImports: new Map([
      ["@earendil-works/pi-coding-agent", "type"],
      ["node:fs", "runtime"],
    ]),
  },
];

function checkSource(
  path: URL,
  source: string,
  expectedImports: ReadonlyMap<string, string>,
): void {
  const sourceFile = ts.createSourceFile(
    fileURLToPath(path),
    source,
    ts.ScriptTarget.ES2024,
    true,
    ts.ScriptKind.TS,
  );
  const observedImports = new Map<string, string>();

  function visit(node: ts.Node): void {
    if (
      ts.isImportDeclaration(node) &&
      ts.isStringLiteral(node.moduleSpecifier)
    ) {
      const specifier = node.moduleSpecifier.text;
      const expectedKind = expectedImports.get(specifier);
      const actualKind =
        node.importClause?.isTypeOnly === true ? "type" : "runtime";
      if (expectedKind === undefined || expectedKind !== actualKind) {
        throw new Error(
          `Unadmitted runtime import in ${sourceFile.fileName}: ${specifier}`,
        );
      }
      observedImports.set(specifier, actualKind);
    }

    if (
      ts.isCallExpression(node) &&
      (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
        (ts.isIdentifier(node.expression) &&
          node.expression.text === "require"))
    ) {
      throw new Error(
        `Dynamic package lookup is not permitted in ${sourceFile.fileName}.`,
      );
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  assert.deepEqual(observedImports, expectedImports);

  for (const forbiddenText of [
    "node_modules",
    "registry.npmjs.org",
    "npm install",
  ]) {
    if (source.includes(forbiddenText)) {
      throw new Error(
        `Forbidden runtime lookup text in ${sourceFile.fileName}: ${forbiddenText}`,
      );
    }
  }
}

const packageJson = JSON.parse(
  await readFile(new URL("package.json", packageRoot), "utf8"),
) as Record<string, unknown>;
if ("dependencies" in packageJson) {
  throw new Error(
    "The PiJsonV1 extension toolchain must have zero runtime dependencies.",
  );
}

for (const { path, expectedImports } of runtimeSources) {
  checkSource(path, await readFile(path, "utf8"), expectedImports);
}
