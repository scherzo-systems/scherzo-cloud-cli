import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: ["node_modules/**"],
  },
  ...tseslint.configs.recommended,
  {
    files: ["tests/**/*.ts"],
    rules: {
      "no-restricted-globals": [
        "error",
        {
          name: "fetch",
          message: "Unit tests must not contact a network provider.",
        },
        {
          name: "setInterval",
          message: "Unit tests must not poll for success.",
        },
        {
          name: "setTimeout",
          message: "Unit tests must not sleep or use timing for success.",
        },
      ],
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: [
                "@earendil-works/pi-coding-agent",
                "node:child_process",
                "node:http",
                "node:https",
                "node:net",
                "node:tls",
              ],
              message:
                "Unit tests must use injected fakes instead of live processes or networks.",
            },
          ],
        },
      ],
      "no-restricted-properties": [
        "error",
        {
          object: "process",
          property: "env",
          message: "Unit tests must not depend on ambient configuration.",
        },
      ],
    },
  },
);
