use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClaudeCodeConfig {
    pub(crate) model: String,
    pub(crate) effort: ClaudeCodeEffort,
}

// Claude effort is deliberately independent of Pi thinking because the harness contracts differ.
// jscpd:ignore-start
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(crate) enum ClaudeCodeEffort {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "medium")]
    Medium,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "xhigh")]
    XHigh,
    #[serde(rename = "max")]
    Max,
}

impl ClaudeCodeEffort {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}
// jscpd:ignore-end

// Keep this direct decoder local so Claude's closed configuration can evolve independently.
// jscpd:ignore-start
pub(crate) fn resolve_config(value: &Value) -> Option<ClaudeCodeConfig> {
    let config = serde_json::from_value::<ClaudeCodeConfig>(value.clone()).ok()?;
    if config.model.is_empty() {
        return None;
    }

    Some(ClaudeCodeConfig {
        model: config.model,
        effort: config.effort,
    })
}
// jscpd:ignore-end

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_only_the_closed_nonempty_model_and_effort_shape() {
        for (effort, expected) in [
            ("low", ClaudeCodeEffort::Low),
            ("medium", ClaudeCodeEffort::Medium),
            ("high", ClaudeCodeEffort::High),
            ("xhigh", ClaudeCodeEffort::XHigh),
            ("max", ClaudeCodeEffort::Max),
        ] {
            assert_eq!(
                resolve_config(&serde_json::json!({
                    "model": "claude-opus-4-1",
                    "effort": effort,
                })),
                Some(ClaudeCodeConfig {
                    model: "claude-opus-4-1".to_owned(),
                    effort: expected,
                })
            );
        }

        for invalid in [
            serde_json::json!({"effort": "high"}),
            serde_json::json!({"model": "", "effort": "high"}),
            serde_json::json!({"model": "claude-opus-4-1"}),
            serde_json::json!({"model": "claude-opus-4-1", "effort": "off"}),
            serde_json::json!({
                "model": "claude-opus-4-1",
                "effort": "high",
                "fallbackModel": "claude-sonnet-4",
            }),
        ] {
            assert_eq!(resolve_config(&invalid), None);
        }
    }
}
