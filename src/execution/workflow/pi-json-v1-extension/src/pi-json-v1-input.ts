import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { readFileSync } from "node:fs";

// The Rust materializer replaces this string without invoking a formatter.
// prettier-ignore
const GENERATED_CONFIG_JSON = "__SCHERZO_PI_JSON_V1_INPUT_CONFIG_JSON__";

export interface StagedInputConfig {
  marker: string;
  path: string;
}

export interface PiJsonV1InputConfig {
  message?: StagedInputConfig;
  systemPrompt?: StagedInputConfig;
}

interface LoadedInput {
  marker: string;
  text: string;
}

function compactMessageUpdate(event: { assistantMessageEvent: unknown }): void {
  if (
    typeof event.assistantMessageEvent !== "object" ||
    event.assistantMessageEvent === null ||
    Array.isArray(event.assistantMessageEvent)
  ) {
    return;
  }

  const assistantEvent = event.assistantMessageEvent as Record<string, unknown>;
  assistantEvent.scherzoCompact = true;
  delete assistantEvent.partial;
  delete assistantEvent.message;
  if (
    assistantEvent.type === "text_delta" ||
    assistantEvent.type === "thinking_delta"
  ) {
    delete assistantEvent.delta;
  } else if (
    assistantEvent.type === "text_end" ||
    assistantEvent.type === "thinking_end"
  ) {
    delete assistantEvent.content;
  }
}

function loadInput(
  config: StagedInputConfig | undefined,
): LoadedInput | undefined {
  if (config === undefined) {
    return undefined;
  }
  return {
    marker: config.marker,
    text: readFileSync(config.path, "utf8"),
  };
}

function replaceSingleMarker(
  value: string,
  input: LoadedInput,
): string | undefined {
  const markerIndex = value.indexOf(input.marker);
  if (markerIndex < 0 || markerIndex !== value.lastIndexOf(input.marker)) {
    return undefined;
  }
  return `${value.slice(0, markerIndex)}${input.text}${value.slice(
    markerIndex + input.marker.length,
  )}`;
}

export function createPiJsonV1InputExtension(
  config: PiJsonV1InputConfig,
): (pi: ExtensionAPI) => void {
  const message = loadInput(config.message);
  const systemPrompt = loadInput(config.systemPrompt);

  return (pi) => {
    pi.on("message_update", compactMessageUpdate);

    if (message !== undefined) {
      let pending = true;
      pi.on("input", (event) => {
        if (!pending) {
          return { action: "continue" };
        }
        pending = false;
        if (!event.text.endsWith(message.marker)) {
          return { action: "handled" };
        }
        return {
          action: "transform",
          text: `${event.text.slice(0, -message.marker.length)}${message.text}`,
        };
      });
    }

    if (systemPrompt !== undefined) {
      pi.on("before_agent_start", (event, ctx) => {
        const replaced = replaceSingleMarker(event.systemPrompt, systemPrompt);
        if (replaced === undefined) {
          ctx.abort();
          return;
        }
        return { systemPrompt: replaced };
      });
    }
  };
}

const generatedConfig = JSON.parse(
  GENERATED_CONFIG_JSON,
) as PiJsonV1InputConfig;
export default createPiJsonV1InputExtension(generatedConfig);
