import type {
  AgentToolUpdateCallback,
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { createConnection } from "node:net";
import { Type } from "typebox";

// The materializer replaces this string without invoking a formatter.
// prettier-ignore
const GENERATED_CONFIG_JSON = "{\"toolName\":\"scherzo_result_8f3c2a1d\",\"socketPath\":\"/tmp/scherzo/invocation-fixed/result-validation.sock\",\"parameters\":{\"$defs\":{\"workflowResult\":{\"$defs\":{\"Change\":{\"additionalProperties\":false,\"properties\":{\"summary\":{\"type\":\"string\"}},\"required\":[\"summary\"],\"type\":\"object\"}},\"$id\":\"https://schemas.scherzo.invalid/workflow-result/d9925fc96e15d98f45c13b1f9c14b30482dd085671195c4d01461b165f5dd296\",\"$ref\":\"#/$defs/Change\",\"$schema\":\"https://json-schema.org/draft/2020-12/schema\"}},\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"additionalProperties\":false,\"properties\":{\"result\":{\"$ref\":\"https://schemas.scherzo.invalid/workflow-result/d9925fc96e15d98f45c13b1f9c14b30482dd085671195c4d01461b165f5dd296\"}},\"required\":[\"result\"],\"type\":\"object\"}}";
const MAX_PROTOCOL_FRAME_BYTES = 16 * 1024 * 1024;

export interface PiJsonV1ExtensionConfig {
  toolName: string;
  socketPath: string;
  parameters: Record<string, unknown>;
}

export interface ValidatePiResultV1Request {
  kind: "ValidatePiResultV1";
  toolCallId: string;
  toolName: string;
  arguments: ResultArguments;
}

export type ValidatePiResultV1Response =
  | { kind: "Valid" }
  | { kind: "Rejected"; feedback: string }
  | { kind: "Fatal"; cause: string };

export type ResultValidator = (
  socketPath: string,
  request: ValidatePiResultV1Request,
  signal: AbortSignal | undefined,
) => Promise<ValidatePiResultV1Response>;

interface ResultArguments {
  result: unknown;
}

interface ToolCallBlock {
  id: string;
  name: string;
  arguments?: unknown;
}

type ResultToolCallGroup =
  { kind: "singleton" } | { kind: "sibling" } | { kind: "uncorrelated" };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isToolCallBlock(
  value: unknown,
): value is ToolCallBlock & Record<string, unknown> {
  return (
    isRecord(value) &&
    value.type === "toolCall" &&
    typeof value.id === "string" &&
    typeof value.name === "string"
  );
}

function isResultArguments(value: unknown): value is ResultArguments {
  return (
    isRecord(value) &&
    Object.keys(value).length === 1 &&
    Object.hasOwn(value, "result")
  );
}

function hasOnlyKeys(
  value: Record<string, unknown>,
  keys: readonly string[],
): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && keys.every((key) => key in value);
}

function classifyResultToolCallGroup(
  ctx: ExtensionContext,
  toolCallId: string,
  toolName: string,
): ResultToolCallGroup {
  const entries: readonly unknown[] = ctx.sessionManager.getBranch();
  let candidate: ResultToolCallGroup | undefined;

  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (
      !isRecord(entry) ||
      entry.type !== "message" ||
      !isRecord(entry.message)
    ) {
      continue;
    }

    const message = entry.message;
    if (message.role !== "assistant" || !Array.isArray(message.content)) {
      continue;
    }

    const toolCalls = message.content.filter(isToolCallBlock);
    const matchingCalls = toolCalls.filter((call) => call.id === toolCallId);
    if (matchingCalls.length === 0) {
      continue;
    }
    if (candidate !== undefined || matchingCalls.length !== 1) {
      return { kind: "uncorrelated" };
    }

    const toolCall = matchingCalls[0];
    if (toolCall === undefined || toolCall.name !== toolName) {
      return { kind: "uncorrelated" };
    }
    if (toolCalls.length !== 1) {
      candidate = { kind: "sibling" };
      continue;
    }
    if (!isResultArguments(toolCall.arguments)) {
      return { kind: "uncorrelated" };
    }

    candidate = { kind: "singleton" };
  }

  return candidate ?? { kind: "uncorrelated" };
}

function parseValidationResponse(payload: Buffer): ValidatePiResultV1Response {
  const parsed: unknown = JSON.parse(payload.toString("utf8"));
  if (!isRecord(parsed) || typeof parsed.kind !== "string") {
    throw new Error("The result validator returned an invalid response.");
  }

  if (parsed.kind === "Valid" && hasOnlyKeys(parsed, ["kind"])) {
    return { kind: "Valid" };
  }
  if (
    parsed.kind === "Rejected" &&
    hasOnlyKeys(parsed, ["kind", "feedback"]) &&
    typeof parsed.feedback === "string"
  ) {
    return { kind: "Rejected", feedback: parsed.feedback };
  }
  if (
    parsed.kind === "Fatal" &&
    hasOnlyKeys(parsed, ["kind", "cause"]) &&
    typeof parsed.cause === "string"
  ) {
    return { kind: "Fatal", cause: parsed.cause };
  }

  throw new Error("The result validator returned an invalid response.");
}

export function encodeFrame(value: unknown): Buffer {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  if (payload.byteLength > MAX_PROTOCOL_FRAME_BYTES) {
    throw new Error(
      "The result-validation request exceeds the protocol frame limit.",
    );
  }

  const frame = Buffer.allocUnsafe(4 + payload.byteLength);
  frame.writeUInt32BE(payload.byteLength, 0);
  payload.copy(frame, 4);
  return frame;
}

export function decodeFrame(frame: Buffer): Buffer {
  if (frame.byteLength < 4) {
    throw new Error("The result validator returned a truncated frame.");
  }

  const payloadLength = frame.readUInt32BE(0);
  if (payloadLength > MAX_PROTOCOL_FRAME_BYTES) {
    throw new Error("The result validator returned an oversized frame.");
  }
  if (frame.byteLength !== payloadLength + 4) {
    throw new Error("The result validator returned an invalid frame length.");
  }

  return frame.subarray(4);
}

const validateOverSocket: ResultValidator = (socketPath, request, signal) =>
  new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    const chunks: Buffer[] = [];
    let receivedBytes = 0;
    let settled = false;

    const removeAbortListener = (): void => {
      signal?.removeEventListener("abort", handleAbort);
    };
    const fail = (error: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      removeAbortListener();
      socket.destroy();
      reject(error);
    };
    const handleAbort = (): void => {
      fail(new Error("Result validation was cancelled."));
    };

    if (signal?.aborted) {
      handleAbort();
      return;
    }
    signal?.addEventListener("abort", handleAbort, { once: true });

    socket.once("connect", () => {
      try {
        // Pi 0.84's Bun runtime closes both socket halves on end(), so the
        // length-bounded request is written without a request-side EOF.
        socket.write(encodeFrame(request));
      } catch (error: unknown) {
        fail(
          error instanceof Error
            ? error
            : new Error("Could not encode result validation."),
        );
      }
    });
    socket.on("data", (chunk) => {
      receivedBytes += chunk.byteLength;
      if (receivedBytes > MAX_PROTOCOL_FRAME_BYTES + 4) {
        fail(new Error("The result validator returned an oversized frame."));
        return;
      }
      chunks.push(chunk);
    });
    socket.once("end", () => {
      if (settled) {
        return;
      }

      try {
        const response = parseValidationResponse(
          decodeFrame(Buffer.concat(chunks)),
        );
        settled = true;
        removeAbortListener();
        resolve(response);
      } catch (error: unknown) {
        fail(
          error instanceof Error
            ? error
            : new Error("Could not decode result validation."),
        );
      }
    });
    socket.once("error", (error) => {
      fail(error);
    });
  });

export function createResultTool(
  config: PiJsonV1ExtensionConfig,
  validate: ResultValidator,
  onAccepted: () => void = () => {},
) {
  const parameters = Type.Unsafe<ResultArguments>(config.parameters);

  return {
    name: config.toolName,
    label: "Submit workflow result",
    description:
      "Submit the final workflow result. Call this tool by itself as the final action; a valid result ends the step.",
    parameters,
    async execute(
      toolCallId: string,
      argumentsValue: ResultArguments,
      signal: AbortSignal | undefined,
      _onUpdate: AgentToolUpdateCallback | undefined,
      ctx: ExtensionContext,
    ) {
      let response: ValidatePiResultV1Response;
      try {
        response = await validate(
          config.socketPath,
          {
            kind: "ValidatePiResultV1",
            toolCallId,
            toolName: config.toolName,
            arguments: argumentsValue,
          },
          signal,
        );
      } catch (cause: unknown) {
        ctx.abort();
        throw new Error(
          "Fatal result-validation failure: the validation channel failed.",
          { cause },
        );
      }

      if (response.kind === "Rejected") {
        throw new Error(response.feedback);
      }
      if (response.kind === "Fatal") {
        ctx.abort();
        throw new Error(`Fatal result-validation failure: ${response.cause}`);
      }

      onAccepted();
      return {
        content: [
          { type: "text" as const, text: "Final workflow result accepted." },
        ],
        details: {},
        terminate: true,
      };
    },
  };
}

export function createPiJsonV1Extension(
  config: PiJsonV1ExtensionConfig,
  validate: ResultValidator = validateOverSocket,
): (pi: ExtensionAPI) => void {
  return (pi) => {
    const seenResultToolCallIds = new Set<string>();
    let acceptedResult = false;

    pi.on("tool_call", (event, ctx) => {
      if (event.toolName !== config.toolName) {
        return;
      }

      if (seenResultToolCallIds.has(event.toolCallId)) {
        return {
          block: true,
          reason:
            "No result was accepted. The workflow result call identity was reused.",
        };
      }
      seenResultToolCallIds.add(event.toolCallId);

      const group = classifyResultToolCallGroup(
        ctx,
        event.toolCallId,
        config.toolName,
      );
      if (group.kind === "singleton") {
        return;
      }

      return {
        block: true,
        reason:
          group.kind === "sibling"
            ? "No result was accepted. Call the workflow result tool by itself, without sibling tool calls."
            : "No result was accepted. The workflow result call could not be correlated.",
      };
    });

    pi.on("session_before_compact", () =>
      acceptedResult ? { cancel: true } : undefined,
    );

    pi.registerTool(
      createResultTool(config, validate, () => {
        acceptedResult = true;
      }),
    );
  };
}

const generatedConfig = JSON.parse(
  GENERATED_CONFIG_JSON,
) as PiJsonV1ExtensionConfig;
export default createPiJsonV1Extension(generatedConfig);
