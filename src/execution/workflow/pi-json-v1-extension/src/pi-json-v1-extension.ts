import type {
  AgentToolUpdateCallback,
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { createHash } from "node:crypto";
import { createConnection } from "node:net";
import { Type } from "typebox";

// The materializer replaces this string without invoking a formatter.
// prettier-ignore
const GENERATED_CONFIG_JSON = "__SCHERZO_PI_JSON_V1_CONFIG_JSON__";
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

type ResultArgumentsConsumer = (
  toolCallId: string,
) => ResultArguments | undefined;

type ResultToolCallGroup =
  | { kind: "singleton"; arguments: ResultArguments }
  | { kind: "sibling" }
  | { kind: "uncorrelated" };

const RESULT_DIGEST_PROPERTY = "scherzoResultSha256";

function compareUtf8(left: string, right: string): number {
  return Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) {
      throw new Error("The result candidate is not JSON serializable.");
    }
    return encoded;
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }

  const object = value as Record<string, unknown>;
  return `{${Object.keys(object)
    .sort(compareUtf8)
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`)
    .join(",")}}`;
}

export function resultDigest(argumentsValue: ResultArguments): string {
  return createHash("sha256")
    .update(canonicalJson(argumentsValue), "utf8")
    .digest("hex");
}

function resultToolCalls(value: unknown, toolName: string): ToolCallBlock[] {
  if (!isRecord(value) || !Array.isArray(value.content)) {
    return [];
  }
  return value.content
    .filter(isToolCallBlock)
    .filter((call) => call.name === toolName);
}

function captureResultCalls(
  value: unknown,
  toolName: string,
  captured: Map<string, ResultArguments>,
): void {
  const calls = isToolCallBlock(value)
    ? value.name === toolName
      ? [value]
      : []
    : resultToolCalls(value, toolName);
  for (const call of calls) {
    if (isResultArguments(call.arguments)) {
      captured.set(call.id, call.arguments);
    }
  }
}

function captureAndRedactResultCalls(
  value: unknown,
  toolName: string,
  captured: Map<string, ResultArguments>,
): void {
  const calls = isToolCallBlock(value)
    ? value.name === toolName
      ? [value]
      : []
    : resultToolCalls(value, toolName);
  for (const call of calls) {
    if (isResultArguments(call.arguments)) {
      captured.set(call.id, call.arguments);
    }
    const argumentsValue = captured.get(call.id);
    if (argumentsValue !== undefined) {
      call.arguments = {
        [RESULT_DIGEST_PROPERTY]: resultDigest(argumentsValue),
      };
    }
  }
}

function classifyFinalResultCalls(
  message: unknown,
  toolName: string,
  captured: Map<string, ResultArguments>,
): Map<string, ResultToolCallGroup> {
  const groups = new Map<string, ResultToolCallGroup>();
  if (!isRecord(message) || !Array.isArray(message.content)) {
    return groups;
  }
  const toolCalls = message.content.filter(isToolCallBlock);
  const matchingCalls = toolCalls.filter((call) => call.name === toolName);
  for (const call of matchingCalls) {
    const argumentsValue = isResultArguments(call.arguments)
      ? call.arguments
      : captured.get(call.id);
    groups.set(
      call.id,
      toolCalls.length !== 1
        ? { kind: "sibling" }
        : argumentsValue === undefined
          ? { kind: "uncorrelated" }
          : { kind: "singleton", arguments: argumentsValue },
    );
  }
  return groups;
}

function replaceToolInput(
  input: Record<string, unknown>,
  argumentsValue: ResultArguments,
): void {
  for (const key of Object.keys(input)) {
    delete input[key];
  }
  Object.assign(input, argumentsValue);
}

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

    candidate = { kind: "singleton", arguments: toolCall.arguments };
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
        // Pi 0.83's Bun runtime closes both socket halves on end(), so the
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
  consumeResultArguments: ResultArgumentsConsumer,
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
      _argumentsValue: ResultArguments,
      signal: AbortSignal | undefined,
      _onUpdate: AgentToolUpdateCallback | undefined,
      ctx: ExtensionContext,
    ) {
      const resultArguments = consumeResultArguments(toolCallId);
      if (resultArguments === undefined) {
        ctx.abort();
        throw new Error(
          "Fatal result-validation failure: the assistant result candidate could not be correlated.",
        );
      }

      let response: ValidatePiResultV1Response;
      try {
        response = await validate(
          config.socketPath,
          {
            kind: "ValidatePiResultV1",
            toolCallId,
            toolName: config.toolName,
            arguments: resultArguments,
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
    const capturedResultArguments = new Map<string, ResultArguments>();
    const finalizedResultGroups = new Map<string, ResultToolCallGroup>();
    const resultArgumentsByToolCallId = new Map<string, ResultArguments>();
    const seenResultToolCallIds = new Set<string>();
    const consumeResultArguments = (
      toolCallId: string,
    ): ResultArguments | undefined => {
      const resultArguments = resultArgumentsByToolCallId.get(toolCallId);
      resultArgumentsByToolCallId.delete(toolCallId);
      return resultArguments;
    };

    pi.on("message_update", (event) => {
      const assistantEvent = event.assistantMessageEvent as unknown as Record<
        string,
        unknown
      >;
      const contentIndex = assistantEvent.contentIndex;
      const content = isRecord(event.message)
        ? event.message.content
        : undefined;
      const updatedCall =
        typeof contentIndex === "number" && Array.isArray(content)
          ? content[contentIndex]
          : undefined;
      const isResultUpdate =
        isToolCallBlock(updatedCall) && updatedCall.name === config.toolName;

      for (const value of [
        event.message,
        assistantEvent.partial,
        assistantEvent.toolCall,
        assistantEvent.message,
      ]) {
        captureResultCalls(value, config.toolName, capturedResultArguments);
      }
      for (const value of [
        event.message,
        assistantEvent.partial,
        assistantEvent.toolCall,
        assistantEvent.message,
      ]) {
        captureAndRedactResultCalls(
          value,
          config.toolName,
          capturedResultArguments,
        );
      }
      if (isResultUpdate && typeof assistantEvent.delta === "string") {
        assistantEvent.delta = "";
      }
    });

    pi.on("message_end", (event) => {
      if (event.message.role !== "assistant") {
        return;
      }
      for (const [toolCallId, group] of classifyFinalResultCalls(
        event.message,
        config.toolName,
        capturedResultArguments,
      )) {
        finalizedResultGroups.set(toolCallId, group);
      }
    });

    pi.on("tool_call", (event, ctx) => {
      if (event.toolName !== config.toolName) {
        return;
      }

      if (seenResultToolCallIds.has(event.toolCallId)) {
        resultArgumentsByToolCallId.delete(event.toolCallId);
        return {
          block: true,
          reason:
            "No result was accepted. The workflow result call identity was reused.",
        };
      }
      seenResultToolCallIds.add(event.toolCallId);

      const group =
        finalizedResultGroups.get(event.toolCallId) ??
        classifyResultToolCallGroup(ctx, event.toolCallId, config.toolName);
      finalizedResultGroups.delete(event.toolCallId);
      capturedResultArguments.delete(event.toolCallId);
      if (group.kind === "singleton") {
        resultArgumentsByToolCallId.set(event.toolCallId, group.arguments);
        replaceToolInput(event.input, group.arguments);
        return;
      }

      resultArgumentsByToolCallId.delete(event.toolCallId);
      return {
        block: true,
        reason:
          group.kind === "sibling"
            ? "No result was accepted. Call the workflow result tool by itself, without sibling tool calls."
            : "No result was accepted. The workflow result call could not be correlated.",
      };
    });

    pi.registerTool(createResultTool(config, validate, consumeResultArguments));
  };
}

const generatedConfig = JSON.parse(
  GENERATED_CONFIG_JSON,
) as PiJsonV1ExtensionConfig;
export default createPiJsonV1Extension(generatedConfig);
