#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClaudeCodeConfig {
    pub(crate) model: String,
    pub(crate) effort: ClaudeCodeEffort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClaudeCodeEffort {
    Low,
    Medium,
    High,
    XHigh,
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
