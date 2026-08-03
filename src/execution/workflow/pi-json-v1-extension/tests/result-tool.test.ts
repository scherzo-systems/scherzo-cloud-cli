import assert from "node:assert/strict";
import test from "node:test";

import {
  createPiJsonV1Extension,
  createResultTool,
  decodeFrame,
  encodeFrame,
  type PiJsonV1ExtensionConfig,
  type ValidatePiResultV1Request,
} from "../fixtures/generated/pi-json-v1-extension.ts";

const fixedConfig: PiJsonV1ExtensionConfig = {
  toolName: "scherzo_result_unit_fixed",
  socketPath: "/tmp/scherzo/unit-fixed/result.sock",
  parameters: {
    type: "object",
    properties: {
      result: { type: "object" },
    },
    required: ["result"],
    additionalProperties: false,
  },
};

const fixedPayload = {
  summary: "fixed payload",
  files: ["src/main.rs"],
};

type ExtensionApi = Parameters<ReturnType<typeof createPiJsonV1Extension>>[0];
type ResultValidator = NonNullable<
  Parameters<typeof createPiJsonV1Extension>[1]
>;
type ResultToolContext = Parameters<
  ReturnType<typeof createResultTool>["execute"]
>[4];
type ToolCallGate = (
  event: {
    toolCallId: string;
    toolName: string;
    input: Record<string, unknown>;
  },
  context: { sessionManager: { getBranch(): readonly unknown[] } },
) => unknown;
interface RegisteredResultTool {
  name: string;
  execute(
    toolCallId: string,
    argumentsValue: { result: unknown },
    signal: AbortSignal | undefined,
    onUpdate: undefined,
    context: { abort(): void },
  ): Promise<{ terminate?: boolean }>;
}

function loadResultExtension(
  config: PiJsonV1ExtensionConfig,
  validate: ResultValidator,
): { gate: ToolCallGate; tool: RegisteredResultTool } {
  let gate: ToolCallGate | undefined;
  let tool: RegisteredResultTool | undefined;
  const pi = {
    on(eventName: string, handler: unknown) {
      assert.equal(eventName, "tool_call");
      gate = handler as ToolCallGate;
    },
    registerTool(registeredTool: unknown) {
      tool = registeredTool as RegisteredResultTool;
    },
  } as unknown as ExtensionApi;

  createPiJsonV1Extension(config, validate)(pi);
  assert.ok(gate !== undefined);
  assert.ok(tool !== undefined);
  return { gate, tool };
}

function toolCallContext(content: readonly Record<string, unknown>[]): {
  sessionManager: { getBranch(): readonly unknown[] };
} {
  return {
    sessionManager: {
      getBranch: () => [
        {
          type: "message",
          message: { role: "assistant", content },
        },
      ],
    },
  };
}

function resultToolContext(abort: () => void): ResultToolContext {
  return { abort } as ResultToolContext;
}

test("a valid fixed payload produces a terminating tool result", async () => {
  let capturedRequest: ValidatePiResultV1Request | undefined;
  const tool = createResultTool(
    fixedConfig,
    async (socketPath, request, signal) => {
      assert.equal(socketPath, fixedConfig.socketPath);
      assert.equal(signal, undefined);
      capturedRequest = request;
      return { kind: "Valid" };
    },
    () => ({ result: fixedPayload }),
  );

  const result = await tool.execute(
    "tool-call-fixed",
    { result: fixedPayload },
    undefined,
    undefined,
    resultToolContext(() => assert.fail("valid results must not abort Pi")),
  );

  assert.deepEqual(capturedRequest, {
    kind: "ValidatePiResultV1",
    toolCallId: "tool-call-fixed",
    toolName: fixedConfig.toolName,
    arguments: { result: fixedPayload },
  });
  assert.equal(result.terminate, true);
});

test("validator feedback is returned as a tool error", async () => {
  const tool = createResultTool(
    fixedConfig,
    async () => ({
      kind: "Rejected",
      feedback: "The fixed payload does not satisfy the retained schema.",
    }),
    () => ({ result: fixedPayload }),
  );

  await assert.rejects(
    tool.execute(
      "tool-call-fixed",
      { result: fixedPayload },
      undefined,
      undefined,
      resultToolContext(() =>
        assert.fail("rejected results must not abort Pi"),
      ),
    ),
    new Error("The fixed payload does not satisfy the retained schema."),
  );
});

test("validation sends the assistant's uncoerced candidate to Rust", async () => {
  const config: PiJsonV1ExtensionConfig = {
    ...fixedConfig,
    toolName: "scherzo_result_coercion_fixed",
    parameters: {
      type: "object",
      properties: { result: { type: "number" } },
      required: ["result"],
      additionalProperties: false,
    },
  };
  const assistantArguments = { result: "42" };
  const piCoercedArguments = { result: 42 };
  let capturedRequest: ValidatePiResultV1Request | undefined;
  const { gate, tool } = loadResultExtension(
    config,
    async (_socketPath, request) => {
      capturedRequest = request;
      return { kind: "Valid" };
    },
  );
  const toolCallId = "tool-call-coercion-fixed";

  assert.equal(
    gate(
      {
        toolCallId,
        toolName: config.toolName,
        input: piCoercedArguments,
      },
      toolCallContext([
        {
          type: "toolCall",
          id: toolCallId,
          name: config.toolName,
          arguments: assistantArguments,
        },
      ]),
    ),
    undefined,
  );
  await tool.execute(toolCallId, piCoercedArguments, undefined, undefined, {
    abort() {},
  });

  assert.deepEqual(capturedRequest?.arguments, assistantArguments);
});

test("the extension blocks a result call with a sibling tool call", () => {
  const { gate, tool } = loadResultExtension(fixedConfig, async () => ({
    kind: "Valid",
  }));
  const toolCallId = "tool-call-with-sibling-fixed";

  assert.equal(tool.name, fixedConfig.toolName);
  const gateResult = gate(
    {
      toolCallId,
      toolName: fixedConfig.toolName,
      input: { result: fixedPayload },
    },
    toolCallContext([
      {
        type: "toolCall",
        id: toolCallId,
        name: fixedConfig.toolName,
        arguments: { result: fixedPayload },
      },
      {
        type: "toolCall",
        id: "sibling-tool-call-fixed",
        name: "read",
        arguments: { path: "fixed-input.txt" },
      },
    ]),
  );
  assert.ok(gateResult !== null && typeof gateResult === "object");
  assert.equal((gateResult as { block?: unknown }).block, true);
});

test("a fatal validator response aborts Pi before surfacing the failure", async () => {
  let aborted = false;
  const { gate, tool } = loadResultExtension(fixedConfig, async () => ({
    kind: "Fatal",
    cause: "The fixed validation budget was exhausted.",
  }));
  const toolCallId = "tool-call-fatal-fixed";
  const argumentsValue = { result: fixedPayload };

  assert.equal(
    gate(
      {
        toolCallId,
        toolName: fixedConfig.toolName,
        input: argumentsValue,
      },
      toolCallContext([
        {
          type: "toolCall",
          id: toolCallId,
          name: fixedConfig.toolName,
          arguments: argumentsValue,
        },
      ]),
    ),
    undefined,
  );
  await assert.rejects(
    tool.execute(toolCallId, argumentsValue, undefined, undefined, {
      abort() {
        aborted = true;
      },
    }),
  );

  assert.equal(aborted, true);
});

test("missing assistant correlation aborts before validation", async () => {
  let aborted = false;
  let validated = false;
  const tool = createResultTool(
    fixedConfig,
    async () => {
      validated = true;
      return { kind: "Valid" };
    },
    () => undefined,
  );

  await assert.rejects(
    tool.execute(
      "tool-call-missing-fixed",
      { result: fixedPayload },
      undefined,
      undefined,
      resultToolContext(() => {
        aborted = true;
      }),
    ),
  );

  assert.equal(aborted, true);
  assert.equal(validated, false);
});

test("protocol framing preserves a fixed request payload", () => {
  const request: ValidatePiResultV1Request = {
    kind: "ValidatePiResultV1",
    toolCallId: "tool-call-fixed",
    toolName: fixedConfig.toolName,
    arguments: { result: fixedPayload },
  };

  const decoded: unknown = JSON.parse(
    decodeFrame(encodeFrame(request)).toString("utf8"),
  );
  assert.deepEqual(decoded, request);
});
