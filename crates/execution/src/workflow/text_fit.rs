use unicode_segmentation::UnicodeSegmentation as _;
use unicode_width::UnicodeWidthStr;

pub(super) fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

pub(super) fn ellipsize(value: &str, width: usize) -> String {
    if display_width(value) <= width {
        return value.to_owned();
    }
    if width == 0 {
        return String::new();
    }
    let mut result = String::new();
    let mut used = 0_usize;
    for grapheme in value.graphemes(true) {
        let length = display_width(grapheme);
        if used.saturating_add(length) > width - 1 {
            break;
        }
        result.push_str(grapheme);
        used = used.saturating_add(length);
    }
    result.push('…');
    result
}

pub(super) fn fit_text(value: &str, width: usize) -> String {
    ellipsize(value, width)
}

// The plain view wraps at word boundaries where possible, but never inside a grapheme.
pub(super) fn next_detail_segment(value: &str, maximum_width: usize) -> (&str, &str) {
    let maximum_width = maximum_width.max(1);
    if display_width(value) <= maximum_width {
        return (value, "");
    }

    let mut used_width = 0_usize;
    let mut fitting_end = 0;
    for (index, grapheme) in value.grapheme_indices(true) {
        let width = display_width(grapheme);
        if used_width.saturating_add(width) > maximum_width {
            if used_width == 0 {
                fitting_end = index + grapheme.len();
            }
            break;
        }
        used_width += width;
        fitting_end = index + grapheme.len();
    }

    let candidate = &value[..fitting_end];
    if value[fitting_end..].starts_with(char::is_whitespace) {
        return (
            candidate.trim_end_matches(char::is_whitespace),
            value[fitting_end..].trim_start_matches(char::is_whitespace),
        );
    }
    if let Some((boundary, whitespace)) =
        candidate
            .grapheme_indices(true)
            .rev()
            .find(|(index, grapheme)| {
                grapheme.starts_with(char::is_whitespace)
                    && *index != 0
                    && candidate[..*index]
                        .chars()
                        .any(|character| !character.is_whitespace())
            })
    {
        return (
            candidate[..boundary].trim_end_matches(char::is_whitespace),
            value[boundary + whitespace.len()..].trim_start_matches(char::is_whitespace),
        );
    }
    (candidate, &value[fitting_end..])
}

// Scroll offsets must land at the end of a complete grapheme, never inside one.
pub(super) fn next_grapheme_boundary(value: &str, start: usize, target: usize) -> usize {
    if target <= start {
        return target;
    }
    let mut boundary = start;
    for grapheme in value.graphemes(true) {
        if boundary >= target {
            break;
        }
        boundary = boundary.saturating_add(display_width(grapheme));
    }
    boundary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fitting_and_panning_keep_combining_clusters_intact() {
        assert_eq!(fit_text("ae\u{301}bc", 2), "a…");
        assert_eq!(ellipsize("e\u{301}bc", 2), "e\u{301}…");
        assert_eq!(next_grapheme_boundary("a👩‍🚀bc", 0, 3), 3);
        assert_eq!(next_detail_segment("a👩‍🚀bc", 2), ("a", "👩‍🚀bc"));
        assert_eq!(next_detail_segment("a👩‍🚀bc", 3), ("a👩‍🚀", "bc"));
    }
}
