use super::evidence::InheritedDetail;

pub(super) const PRIMARY: (u8, u8, u8) = (205, 214, 244);
pub(super) const NEUTRAL: (u8, u8, u8) = (186, 194, 222);
pub(super) const MUTED: (u8, u8, u8) = (127, 132, 156);
pub(super) const ACTIVE: (u8, u8, u8) = (137, 180, 250);
pub(super) const SUCCESS: (u8, u8, u8) = (166, 227, 161);
pub(super) const FAILURE: (u8, u8, u8) = (243, 139, 168);
pub(super) const BLOCKED: (u8, u8, u8) = (250, 179, 135);
pub(super) const SELECTION: (u8, u8, u8) = (49, 50, 68);
pub(super) const SEPARATOR: (u8, u8, u8) = (69, 71, 90);
pub(super) const ACCENT: (u8, u8, u8) = (203, 166, 247);
pub(super) const FOOTER_KEY: (u8, u8, u8) = (180, 190, 254);
pub(super) const HELP_KEY: (u8, u8, u8) = (249, 226, 175);

// ANSI and RGB representations live together so the plain and TUI palettes evolve together.
pub(super) const STYLE_PRIMARY: &str = "38;2;205;214;244";
pub(super) const STYLE_SECONDARY: &str = "38;2;166;173;200";
pub(super) const STYLE_MUTED: &str = "38;2;127;132;156";
pub(super) const STYLE_ACTIVE: &str = "38;2;137;180;250";
pub(super) const STYLE_OUTPUT: &str = "38;2;148;226;213";
pub(super) const STYLE_SUCCESS: &str = "38;2;166;227;161";
pub(super) const STYLE_FAILURE: &str = "38;2;243;139;168";
pub(super) const STYLE_BLOCKED: &str = "38;2;250;179;135";
pub(super) const STYLE_CONTINUATION: &str = "2;38;2;127;132;156";

pub(super) fn success_detail(command: bool, count: usize) -> String {
    match (command, count) {
        (true, 0) => "exit 0".to_owned(),
        (true, _) => format!(
            "exit 0 · {count} {}",
            if count == 1 { "output" } else { "outputs" }
        ),
        (false, _) => format!(
            "{count} {} committed",
            if count == 1 { "output" } else { "outputs" }
        ),
    }
}

pub(super) fn inherited_detail(detail: &InheritedDetail) -> String {
    format!(
        "prior attempt {} ({:?}) · definition changed {}",
        detail.prior_attempt_number, detail.prior_state, detail.definition_changed,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_text_is_independent_of_renderer() {
        assert_eq!(success_detail(true, 0), "exit 0");
        assert_eq!(success_detail(true, 2), "exit 0 · 2 outputs");
        assert_eq!(success_detail(false, 0), "0 outputs committed");
    }
}
