import type { PiJsonV1ExtensionConfig } from "./pi-json-v1-extension.ts";

const CONFIG_MARKER = "__SCHERZO_PI_JSON_V1_CONFIG_JSON__";
const ENCODED_CONFIG_MARKER = JSON.stringify(CONFIG_MARKER);

export function materializePiJsonV1Extension(
  template: string,
  config: PiJsonV1ExtensionConfig,
): string {
  const markerIndex = template.indexOf(ENCODED_CONFIG_MARKER);
  if (
    markerIndex < 0 ||
    markerIndex !== template.lastIndexOf(ENCODED_CONFIG_MARKER)
  ) {
    throw new Error(
      "The PiJsonV1 extension template must contain exactly one config marker.",
    );
  }

  const configJson = JSON.stringify(config);
  if (configJson === undefined) {
    throw new Error("The PiJsonV1 extension config is not JSON serializable.");
  }

  return `${template.slice(0, markerIndex)}${JSON.stringify(configJson)}${template.slice(
    markerIndex + ENCODED_CONFIG_MARKER.length,
  )}`;
}
