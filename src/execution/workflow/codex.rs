use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodexConfig {
    pub(crate) model: String,
    pub(crate) effort: String,
}

// Codex keeps native effort as a string and must not inherit Claude's closed effort enum.
// jscpd:ignore-start
pub(crate) fn resolve_config(value: &Value) -> Option<CodexConfig> {
    let config = serde_json::from_value::<CodexConfig>(value.clone()).ok()?;
    (!config.model.is_empty() && !config.effort.is_empty()).then_some(config)
}
// jscpd:ignore-end

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_configuration_is_exact_and_uses_native_nonempty_effort() {
        // Keep native strings visible rather than coupling this contract to Claude's enum tests.
        // jscpd:ignore-start
        for effort in ["low", "xhigh", "future-native-effort", " "] {
            assert_eq!(
                resolve_config(&serde_json::json!({
                    "model": "gpt-5.4",
                    "effort": effort,
                })),
                Some(CodexConfig {
                    model: "gpt-5.4".to_owned(),
                    effort: effort.to_owned(),
                })
            );
        }
        // jscpd:ignore-end

        for invalid in [
            serde_json::json!({"effort": "high"}),
            serde_json::json!({"model": "", "effort": "high"}),
            serde_json::json!({"model": "gpt-5.4", "effort": ""}),
            serde_json::json!({"model": "gpt-5.4"}),
            serde_json::json!({
                "model": "gpt-5.4",
                "effort": "high",
                "modelProvider": "openai",
            }),
            serde_json::json!({
                "model": "gpt-5.4",
                "effort": "high",
                "apiKey": "credential-is-not-workflow-configuration",
            }),
        ] {
            assert_eq!(resolve_config(&invalid), None);
        }
    }
}
