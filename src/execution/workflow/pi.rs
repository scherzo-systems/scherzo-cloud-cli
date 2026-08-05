use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PiConfig {
    pub(crate) model: String,
    pub(crate) thinking: Thinking,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Thinking {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}

impl Thinking {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PiConfigDto {
    model: String,
    thinking: ThinkingDto,
}

#[derive(Deserialize)]
enum ThinkingDto {
    #[serde(rename = "off")]
    Off,
    #[serde(rename = "minimal")]
    Minimal,
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

pub(crate) fn resolve_config(value: &Value) -> Option<PiConfig> {
    let config = serde_json::from_value::<PiConfigDto>(value.clone()).ok()?;
    if config.model.is_empty() {
        return None;
    }

    Some(PiConfig {
        model: config.model,
        thinking: config.thinking.into(),
    })
}

impl From<ThinkingDto> for Thinking {
    fn from(value: ThinkingDto) -> Self {
        match value {
            ThinkingDto::Off => Self::Off,
            ThinkingDto::Minimal => Self::Minimal,
            ThinkingDto::Low => Self::Low,
            ThinkingDto::Medium => Self::Medium,
            ThinkingDto::High => Self::High,
            ThinkingDto::XHigh => Self::XHigh,
            ThinkingDto::Max => Self::Max,
        }
    }
}
