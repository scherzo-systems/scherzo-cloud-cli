import type {
  AssistantMessage,
  AssistantMessageEventStream,
  Context,
  Model,
  SimpleStreamOptions,
  StopReason,
  ToolCall,
} from "@earendil-works/pi-ai";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { spawn } from "node:child_process";
import { createConnection } from "node:net";
import { Type } from "typebox";

const MAXIMUM_FRAME_BYTES = 16 * 1024 * 1024;
const configuredSocketPath =
  process.env.SCHERZO_PI_FAKE_PROVIDER_SOCKET ??
  process.env.WORKFLOW_RUN_FIXTURE_SOCKET;

if (configuredSocketPath === undefined) {
  throw new Error("A fake-provider control socket is required");
}
const socketPath: string = configuredSocketPath;
const stubbornFixtureExecutable =
  process.env.SCHERZO_PI_STUBBORN_FIXTURE_EXECUTABLE;

interface ResponseUsage {
  inputTokens: number | undefined;
}

interface TextResponse extends ResponseUsage {
  kind: "text";
  blocks: string[];
  stopReason: "stop" | "length";
}

interface ToolCallResponse {
  id: string;
  name: string;
  arguments: Record<string, unknown>;
}

interface ToolCallsResponse extends ResponseUsage {
  kind: "toolCalls";
  calls: ToolCallResponse[];
}

interface StreamedToolCallResponse extends ResponseUsage {
  kind: "streamedToolCall";
  thinking: string[];
  finalizedThinking: string;
  thinkingEndContent: string | undefined;
  call: ToolCallResponse;
}

interface PartialToolCallFailureResponse extends ResponseUsage {
  kind: "partialToolCallFailure";
  thinking: string[];
  call: ToolCallResponse;
  message: string;
}

interface TruncatedToolCallResponse extends ResponseUsage {
  kind: "truncatedToolCall";
  call: ToolCallResponse;
}

interface FailureResponse extends ResponseUsage {
  kind: "failure";
  stopReason: "error" | "aborted";
  message: string;
}

type ProviderResponse =
  | TextResponse
  | ToolCallsResponse
  | StreamedToolCallResponse
  | PartialToolCallFailureResponse
  | TruncatedToolCallResponse
  | FailureResponse;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validInputTokens(value: unknown): boolean {
  return (
    value === undefined ||
    (typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
  );
}

function decodeResponse(value: unknown): ProviderResponse {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new Error(
      "The fake-provider controller returned an invalid response",
    );
  }
  if (
    value.kind === "text" &&
    Array.isArray(value.blocks) &&
    value.blocks.every((block) => typeof block === "string") &&
    (value.stopReason === "stop" || value.stopReason === "length") &&
    validInputTokens(value.inputTokens)
  ) {
    return {
      kind: "text",
      blocks: value.blocks,
      stopReason: value.stopReason,
      inputTokens: value.inputTokens as number | undefined,
    };
  }
  if (
    value.kind === "toolCalls" &&
    Array.isArray(value.calls) &&
    value.calls.every(
      (call) =>
        isRecord(call) &&
        typeof call.id === "string" &&
        typeof call.name === "string" &&
        isRecord(call.arguments),
    ) &&
    validInputTokens(value.inputTokens)
  ) {
    return {
      kind: "toolCalls",
      calls: value.calls as ToolCallsResponse["calls"],
      inputTokens: value.inputTokens as number | undefined,
    };
  }
  if (
    value.kind === "streamedToolCall" &&
    Array.isArray(value.thinking) &&
    value.thinking.every((chunk) => typeof chunk === "string") &&
    typeof value.finalizedThinking === "string" &&
    (value.thinkingEndContent === undefined ||
      typeof value.thinkingEndContent === "string") &&
    isRecord(value.call) &&
    typeof value.call.id === "string" &&
    typeof value.call.name === "string" &&
    isRecord(value.call.arguments) &&
    validInputTokens(value.inputTokens)
  ) {
    return {
      kind: "streamedToolCall",
      thinking: value.thinking,
      finalizedThinking: value.finalizedThinking,
      thinkingEndContent: value.thinkingEndContent as string | undefined,
      inputTokens: value.inputTokens as number | undefined,
      call: {
        id: value.call.id,
        name: value.call.name,
        arguments: value.call.arguments,
      },
    };
  }
  if (
    value.kind === "partialToolCallFailure" &&
    Array.isArray(value.thinking) &&
    value.thinking.every((chunk) => typeof chunk === "string") &&
    isRecord(value.call) &&
    typeof value.call.id === "string" &&
    typeof value.call.name === "string" &&
    isRecord(value.call.arguments) &&
    typeof value.message === "string" &&
    validInputTokens(value.inputTokens)
  ) {
    return {
      kind: "partialToolCallFailure",
      thinking: value.thinking,
      inputTokens: value.inputTokens as number | undefined,
      call: {
        id: value.call.id,
        name: value.call.name,
        arguments: value.call.arguments,
      },
      message: value.message,
    };
  }
  if (
    value.kind === "truncatedToolCall" &&
    isRecord(value.call) &&
    typeof value.call.id === "string" &&
    typeof value.call.name === "string" &&
    isRecord(value.call.arguments) &&
    validInputTokens(value.inputTokens)
  ) {
    return {
      kind: "truncatedToolCall",
      inputTokens: value.inputTokens as number | undefined,
      call: {
        id: value.call.id,
        name: value.call.name,
        arguments: value.call.arguments,
      },
    };
  }
  if (
    value.kind === "failure" &&
    (value.stopReason === "error" || value.stopReason === "aborted") &&
    typeof value.message === "string" &&
    validInputTokens(value.inputTokens)
  ) {
    return {
      kind: "failure",
      stopReason: value.stopReason,
      message: value.message,
      inputTokens: value.inputTokens as number | undefined,
    };
  }
  throw new Error("The fake-provider controller returned an invalid response");
}

function exchange(request: unknown, signal?: AbortSignal): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const payload = Buffer.from(JSON.stringify(request), "utf8");
    if (payload.byteLength > MAXIMUM_FRAME_BYTES) {
      reject(new Error("The fake-provider request exceeded its frame limit"));
      return;
    }

    const frame = Buffer.allocUnsafe(payload.byteLength + 4);
    frame.writeUInt32BE(payload.byteLength, 0);
    payload.copy(frame, 4);

    const socket = createConnection({ path: socketPath, allowHalfOpen: true });
    const chunks: Buffer[] = [];
    let receivedBytes = 0;
    let settled = false;

    const removeAbortListener = (): void => {
      signal?.removeEventListener("abort", abort);
    };
    const fail = (error: Error): void => {
      if (settled) return;
      settled = true;
      removeAbortListener();
      socket.destroy();
      reject(error);
    };
    const abort = (): void => {
      fail(new Error("The fake-provider transition was cancelled"));
    };

    if (signal?.aborted === true) {
      abort();
      return;
    }
    signal?.addEventListener("abort", abort, { once: true });

    socket.once("connect", () => {
      socket.write(frame);
    });
    socket.on("data", (chunk) => {
      receivedBytes += chunk.byteLength;
      if (receivedBytes > MAXIMUM_FRAME_BYTES + 4) {
        fail(new Error("The fake-provider response exceeded its frame limit"));
        return;
      }
      chunks.push(chunk);
    });
    socket.once("end", () => {
      if (settled) return;
      const response = Buffer.concat(chunks);
      if (response.byteLength < 4) {
        fail(new Error("The fake-provider response was truncated"));
        return;
      }
      const responseLength = response.readUInt32BE(0);
      if (
        responseLength > MAXIMUM_FRAME_BYTES ||
        response.byteLength !== responseLength + 4
      ) {
        fail(new Error("The fake-provider response frame was invalid"));
        return;
      }
      try {
        const decoded: unknown = JSON.parse(
          response.subarray(4).toString("utf8"),
        );
        settled = true;
        removeAbortListener();
        resolve(decoded);
      } catch (error: unknown) {
        fail(
          error instanceof Error
            ? error
            : new Error("The fake-provider response was not JSON"),
        );
      }
    });
    socket.once("error", fail);
  });
}

function snapshot(message: AssistantMessage): AssistantMessage {
  return structuredClone(message);
}

function emptyMessage(model: Model<string>): AssistantMessage {
  return {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "pending",
    timestamp: Date.now(),
  };
}

function emitText(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  response: TextResponse,
): void {
  for (const text of response.blocks) {
    const contentIndex = output.content.length;
    output.content.push({ type: "text", text: "" });
    stream.push({
      type: "text_start",
      contentIndex,
      partial: snapshot(output),
    });
    output.content[contentIndex] = { type: "text", text };
    stream.push({
      type: "text_delta",
      contentIndex,
      delta: text,
      partial: snapshot(output),
    });
    stream.push({
      type: "text_end",
      contentIndex,
      content: text,
      partial: snapshot(output),
    });
  }
  output.stopReason = response.stopReason;
  stream.push({
    type: "done",
    reason: response.stopReason,
    message: snapshot(output),
  });
}

function emitToolCalls(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  calls: ToolCallsResponse["calls"],
): void {
  for (const call of calls) {
    const contentIndex = output.content.length;
    const toolCall: ToolCall = {
      type: "toolCall",
      id: call.id,
      name: call.name,
      arguments: {},
    };
    output.content.push(toolCall);
    stream.push({
      type: "toolcall_start",
      contentIndex,
      partial: snapshot(output),
    });
    const delta = JSON.stringify(call.arguments);
    for (let offset = 0; offset < delta.length; offset += 64 * 1024) {
      stream.push({
        type: "toolcall_delta",
        contentIndex,
        delta: delta.slice(offset, offset + 64 * 1024),
        partial: snapshot(output),
      });
    }
    toolCall.arguments = call.arguments;
    stream.push({
      type: "toolcall_end",
      contentIndex,
      toolCall: structuredClone(toolCall),
      partial: snapshot(output),
    });
  }
  output.stopReason = "toolUse";
  stream.push({
    type: "done",
    reason: "toolUse",
    message: snapshot(output),
  });
}

async function emitThinking(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  chunks: string[],
  finalizedThinking?: string,
  thinkingEndContent?: string,
): Promise<void> {
  const contentIndex = output.content.length;
  const thinking = { type: "thinking" as const, thinking: "" };
  output.content.push(thinking);
  stream.push({
    type: "thinking_start",
    contentIndex,
    partial: snapshot(output),
  });
  await Promise.resolve();
  for (const delta of chunks) {
    thinking.thinking += delta;
    stream.push({
      type: "thinking_delta",
      contentIndex,
      delta,
      partial: snapshot(output),
    });
    await Promise.resolve();
  }
  if (finalizedThinking !== undefined) {
    thinking.thinking = finalizedThinking;
  }
  stream.push({
    type: "thinking_end",
    contentIndex,
    content: thinkingEndContent ?? thinking.thinking,
    partial: snapshot(output),
  });
  await Promise.resolve();
}

async function emitOpenToolCall(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  call: ToolCallResponse,
): Promise<{ contentIndex: number; toolCall: ToolCall }> {
  const contentIndex = output.content.length;
  const toolCall: ToolCall = {
    type: "toolCall",
    id: call.id,
    name: call.name,
    arguments: {},
  };
  output.content.push(toolCall);
  stream.push({
    type: "toolcall_start",
    contentIndex,
    partial: snapshot(output),
  });
  await Promise.resolve();
  const encodedArguments = JSON.stringify(call.arguments);
  let partialJson = "";
  for (let offset = 0; offset < encodedArguments.length; offset += 1024) {
    const delta = encodedArguments.slice(offset, offset + 1024);
    partialJson += delta;
    try {
      toolCall.arguments = JSON.parse(partialJson) as Record<string, unknown>;
    } catch {
      // OpenAI Codex exposes the last parseable partial argument object.
    }
    stream.push({
      type: "toolcall_delta",
      contentIndex,
      delta,
      partial: snapshot(output),
    });
    await Promise.resolve();
  }
  toolCall.arguments = call.arguments;
  return { contentIndex, toolCall };
}

async function emitStreamedToolCall(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  response: StreamedToolCallResponse,
): Promise<void> {
  await emitThinking(
    stream,
    output,
    response.thinking,
    response.finalizedThinking,
    response.thinkingEndContent,
  );
  const { contentIndex, toolCall } = await emitOpenToolCall(
    stream,
    output,
    response.call,
  );
  stream.push({
    type: "toolcall_end",
    contentIndex,
    toolCall: structuredClone(toolCall),
    partial: snapshot(output),
  });
  await Promise.resolve();
  output.stopReason = "toolUse";
  stream.push({
    type: "done",
    reason: "toolUse",
    message: snapshot(output),
  });
}

async function emitPartialToolCallFailure(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  response: PartialToolCallFailureResponse,
): Promise<void> {
  await emitThinking(stream, output, response.thinking);
  await emitOpenToolCall(stream, output, response.call);
  output.stopReason = "error";
  output.errorMessage = response.message;
  output.diagnostics = [
    {
      type: "provider_transport_failure",
      timestamp: Date.now(),
      error: { name: "Error", message: response.message },
      details: {
        eventsEmitted: true,
        phase: "after_message_stream_start",
      },
    },
  ];
  stream.push({
    type: "error",
    reason: "error",
    error: snapshot(output),
  });
}

function emitTruncatedToolCall(
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  call: ToolCallResponse,
): void {
  const toolCall: ToolCall = {
    type: "toolCall",
    id: call.id,
    name: call.name,
    arguments: {},
  };
  output.content.push(toolCall);
  stream.push({
    type: "toolcall_start",
    contentIndex: 0,
    partial: snapshot(output),
  });
  const delta = JSON.stringify(call.arguments);
  toolCall.arguments = call.arguments;
  stream.push({
    type: "toolcall_delta",
    contentIndex: 0,
    delta,
    partial: snapshot(output),
  });
  // Pi 0.84 must receive the terminal length event while this tool-call
  // block is still open; emitting toolcall_end would not reproduce its
  // truncated-call recovery path.
  output.stopReason = "length";
  stream.push({
    type: "done",
    reason: "length",
    message: snapshot(output),
  });
}

function streamFakeProvider(
  model: Model<string>,
  context: Context,
  options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output = emptyMessage(model);

  void (async () => {
    try {
      stream.push({ type: "start", partial: snapshot(output) });
      const response = decodeResponse(
        await exchange(
          {
            kind: "model",
            systemPrompt: context.systemPrompt,
            messages: context.messages,
            tools: context.tools,
            reasoning: options?.reasoning,
          },
          options?.signal,
        ),
      );

      if (response.inputTokens !== undefined) {
        output.usage.input = response.inputTokens;
        output.usage.totalTokens = response.inputTokens + output.usage.output;
      }

      if (response.kind === "text") {
        emitText(stream, output, response);
      } else if (response.kind === "toolCalls") {
        emitToolCalls(stream, output, response.calls);
      } else if (response.kind === "streamedToolCall") {
        await emitStreamedToolCall(stream, output, response);
      } else if (response.kind === "partialToolCallFailure") {
        await emitPartialToolCallFailure(stream, output, response);
      } else if (response.kind === "truncatedToolCall") {
        emitTruncatedToolCall(stream, output, response.call);
      } else {
        output.stopReason = response.stopReason;
        output.errorMessage = response.message;
        stream.push({
          type: "error",
          reason: response.stopReason,
          error: snapshot(output),
        });
      }
      stream.end();
    } catch (error: unknown) {
      output.stopReason =
        options?.signal?.aborted === true ? "aborted" : "error";
      output.errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown fake-provider failure";
      stream.push({
        type: "error",
        reason: output.stopReason as Extract<StopReason, "error" | "aborted">,
        error: snapshot(output),
      });
      stream.end();
    }
  })();

  return stream;
}

export default function fakeProviderExtension(pi: ExtensionAPI): void {
  pi.registerProvider("scherzo-fake", {
    name: "Scherzo deterministic fake provider",
    baseUrl: "http://offline.invalid",
    apiKey: "offline-fixture-key",
    api: "scherzo-fake-api",
    models: [
      {
        id: "conformance",
        name: "PiJsonV1 conformance",
        reasoning: true,
        thinkingLevelMap: {
          off: "off",
          minimal: "minimal",
          low: "low",
          medium: "medium",
          high: "high",
          xhigh: "xhigh",
          max: "max",
        },
        input: ["text", "image"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 128_000,
        maxTokens: 4096,
      },
    ],
    streamSimple: streamFakeProvider,
  });

  pi.registerTool({
    name: "conformance_gate",
    label: "Conformance gate",
    description:
      "A deterministic blocking tool used only by PiJsonV1 conformance",
    parameters: Type.Object({ value: Type.String() }),
    async execute(toolCallId, parameters, signal) {
      const response = await exchange(
        {
          kind: "tool",
          toolCallId,
          parameters,
        },
        signal,
      );
      if (!isRecord(response) || response.kind !== "release") {
        throw new Error("The conformance tool received an invalid release");
      }
      return {
        content: [{ type: "text" as const, text: "conformance tool released" }],
        details: {},
      };
    },
  });

  pi.registerTool({
    name: "conformance_stubborn",
    label: "Conformance stubborn descendant",
    description:
      "Starts an interrupt-resistant descendant used only by PiJsonV1 conformance",
    parameters: Type.Object({}),
    async execute(toolCallId, _parameters, signal) {
      if (stubbornFixtureExecutable === undefined) {
        throw new Error("A stubborn-process fixture executable is required");
      }
      const descendant = spawn(
        stubbornFixtureExecutable,
        [
          "--exact",
          "execution::workflow::pi_json_v1::conformance_tests::stubborn_descendant_process_fixture",
          "--ignored",
          "--test-threads=1",
          "--nocapture",
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      const readiness = descendant.stdout;
      const diagnostic = descendant.stderr;
      if (
        descendant.pid === undefined ||
        readiness === null ||
        diagnostic === null
      ) {
        throw new Error("The stubborn conformance descendant did not start");
      }
      await new Promise<void>((resolve, reject) => {
        let startupOutput = "";
        let diagnosticText = "";
        diagnostic.on("data", (data: Buffer) => {
          diagnosticText = (diagnosticText + data.toString("utf8")).slice(
            -1024,
          );
        });
        const cleanup = (): void => {
          descendant.off("error", failed);
          descendant.off("exit", exited);
          readiness.off("data", ready);
        };
        const failed = (error: Error): void => {
          cleanup();
          reject(error);
        };
        const exited = (): void => {
          cleanup();
          reject(
            new Error(
              `The stubborn conformance descendant exited before readiness: ${startupOutput}${diagnosticText}`,
            ),
          );
        };
        const ready = (data: Buffer): void => {
          startupOutput += data.toString("utf8");
          if (startupOutput.includes("SCHERZO_STUBBORN_READY")) {
            cleanup();
            resolve();
            return;
          }
          if (startupOutput.length > 4096) {
            cleanup();
            reject(
              new Error(
                "The stubborn conformance descendant sent invalid readiness",
              ),
            );
          }
        };
        descendant.once("error", failed);
        descendant.once("exit", exited);
        readiness.on("data", ready);
      });
      await exchange(
        {
          kind: "stubborn",
          toolCallId,
          processId: descendant.pid,
        },
        signal,
      );
      return {
        content: [{ type: "text" as const, text: "stubborn tool released" }],
        details: {},
      };
    },
  });

  pi.registerTool({
    name: "conformance_terminate",
    label: "Conformance terminating tool",
    description: "A terminating tool used only by PiJsonV1 conformance",
    parameters: Type.Object({}),
    async execute() {
      return {
        content: [{ type: "text" as const, text: "terminated" }],
        details: {},
        terminate: true,
      };
    },
  });

  pi.on("before_agent_start", async (event, ctx) => {
    const response = await exchange(
      {
        kind: "before_agent_start",
        prompt: event.prompt,
        images: event.images,
        systemPrompt: event.systemPrompt,
        systemPromptOptions: event.systemPromptOptions,
        commands: pi.getCommands(),
        tools: pi.getAllTools(),
        projectTrusted: ctx.isProjectTrusted(),
      },
      ctx.signal,
    );
    if (!isRecord(response) || response.kind !== "release") {
      throw new Error("The startup proof received an invalid release");
    }
  });

  if (process.env.SCHERZO_PI_FAKE_HOLD_SETTLEMENT === "1") {
    pi.on("agent_settled", async () => {
      const response = await exchange({ kind: "settlement" });
      if (!isRecord(response) || response.kind !== "release") {
        throw new Error("The settlement proof received an invalid release");
      }
    });
  }
}
