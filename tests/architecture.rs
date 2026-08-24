//! Architecture boundary tests.
//!
//! These tests codify the module dependency rules described in
//! `ARCHITECTURE.md` so a violation fails a test naming the forbidden
//! edge instead of relying on review to notice it:
//!
//! - "One executable with separate roles" and "Rust source shape": the
//!   top-level module graph is a closed allowlist (`allowed_dependencies`).
//! - "Credential separation": `runner` and `execution` never reference
//!   `human_auth`, and `human_auth` never references `runner`. Both follow
//!   from the allowlist.
//! - "Generated contracts": `api::generated` is referenced only within
//!   `src/api/`.
//! - "Execution boundary": `execution` never references `runner`,
//!   `runner_protocol`, or `cli`, and the runner protocol module stays a
//!   leaf. Both follow from the allowlist.
//! - External crate containment (`external_crate_containment`): command
//!   parsing, HTTP, WebSocket, telemetry, and terminal dependencies stay
//!   inside the modules that own those responsibilities.
//!
//! The scanner is lexical and fails closed: an unreadable source file or an
//! unknown top-level module fails the test rather than being skipped.

#![allow(
    clippy::disallowed_macros,
    reason = "the architecture test resolves the source tree from the Cargo-provided manifest directory"
)]
#![allow(
    clippy::unwrap_used,
    reason = "architecture test failures surface as panics with source context"
)]
#![allow(
    clippy::panic,
    reason = "architecture test failures surface as panics with source context"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Top-level module dependency allowlist. A module may always reference
/// itself; `main.rs` is the composition root and is not constrained.
///
/// Adding an edge here is an architectural decision: update
/// `ARCHITECTURE.md` in the same change when the prose no longer matches.
fn allowed_dependencies() -> BTreeMap<&'static str, BTreeSet<&'static str>> {
    let entries: &[(&str, &[&str])] = &[
        ("api", &["tls", "timing"]),
        (
            "cli",
            &[
                "api",
                "build_info",
                "execution",
                "exit_code",
                "human_auth",
                "idempotency",
                "public_id",
                "runner",
                "timing",
            ],
        ),
        ("build_info", &[]),
        ("error", &["exit_code"]),
        (
            "execution",
            &["build_info", "exit_code", "process", "public_id", "timing"],
        ),
        ("exit_code", &[]),
        ("human_auth", &["api", "timing"]),
        ("idempotency", &[]),
        ("process", &["timing"]),
        ("public_id", &[]),
        (
            "runner",
            &[
                "build_info",
                "execution",
                "idempotency",
                "process",
                "public_id",
                "runner_protocol",
                "timing",
                "tls",
            ],
        ),
        // The runner protocol module is a leaf: DTOs and codecs only.
        ("runner_protocol", &[]),
        // Crate-root test support is a test-only leaf with restricted consumers below.
        ("test_support", &[]),
        ("timing", &[]),
        ("tls", &[]),
    ];
    entries
        .iter()
        .map(|(from, to)| (*from, to.iter().copied().collect()))
        .collect()
}

/// Narrower-than-module targets with their own rules. Longest prefix wins
/// over the plain top-level edge check.
///
/// Both test-support targets are `#[cfg(test)]`-gated, so the compiler
/// already restricts them to test code; these rules decide which modules
/// may share each helper.
fn special_targets() -> Vec<(&'static str, BTreeSet<&'static str>)> {
    vec![
        // "Generated contracts": generated DTOs stay behind the handwritten
        // API boundary.
        ("api::generated", ["api"].into_iter().collect()),
        ("api::test_support", ["api", "runner"].into_iter().collect()),
        (
            "test_support",
            ["execution", "runner"].into_iter().collect(),
        ),
    ]
}

/// External crates whose use is confined to the modules that own the
/// corresponding responsibility. Paths are relative to `src/`.
fn external_crate_containment() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        // Command parsing stays in the typed Clap command tree.
        ("clap", vec!["cli.rs", "cli/"]),
        // HTTP transport stays behind the API client, human auth, and the
        // runner's enrollment, artifact, source, and telemetry transports.
        ("reqwest", vec!["api/", "human_auth/", "runner/"]),
        // The runner WebSocket transport owns the only tungstenite use.
        ("tokio_tungstenite", vec!["runner/service/"]),
        // Runner observability owns the OpenTelemetry SDK surface.
        ("opentelemetry", vec!["runner/"]),
        ("opentelemetry_sdk", vec!["runner/"]),
        ("opentelemetry_proto", vec!["runner/"]),
        // Terminal presentation stays inside workflow execution.
        ("ratatui", vec!["execution/workflow/"]),
        ("crossterm", vec!["execution/workflow/"]),
    ]
}

#[test]
fn module_dependencies_match_architecture() {
    let src = source_root();
    let files = rust_sources(&src);
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}",
        src.display()
    );

    let top_modules = top_level_modules(&src);
    let allowed = allowed_dependencies();
    let specials = special_targets();

    let mut violations = Vec::new();

    for module in &top_modules {
        if module != "main" && !allowed.contains_key(module.as_str()) {
            violations.push(format!(
                "src has top-level module `{module}` with no allowlist entry; \
                 add a deliberate entry to allowed_dependencies()"
            ));
        }
    }
    for (from, targets) in &allowed {
        for name in std::iter::once(from).chain(targets.iter()) {
            if !top_modules.contains(*name) {
                violations.push(format!(
                    "allowlist names `{name}`, which is not a top-level module; \
                     remove the stale rule"
                ));
            }
        }
    }

    for file in &files {
        let relative = file.strip_prefix(&src).unwrap();
        let module_path = module_path_of(relative);
        let Some(from) = module_path.first().cloned() else {
            // `main.rs` and any other crate-root file: composition root.
            continue;
        };
        let text = read_source(file);

        for (line_number, target) in referenced_targets(&text, &module_path, &top_modules) {
            let top = target
                .split_once("::")
                .map_or(target.as_str(), |(first, _)| first);
            if top == from {
                continue;
            }
            if let Some((prefix, allowed_from)) = specials
                .iter()
                .find(|(prefix, _)| is_path_prefix(&target, prefix))
            {
                if !allowed_from.contains(from.as_str()) {
                    violations.push(format!(
                        "{}:{line_number}: `{from}` references `{prefix}`, which is \
                         reserved to {allowed_from:?} (see ARCHITECTURE.md)",
                        relative.display()
                    ));
                }
                continue;
            }
            let permitted = allowed
                .get(from.as_str())
                .is_some_and(|targets| targets.contains(top));
            if !permitted {
                violations.push(format!(
                    "{}:{line_number}: forbidden dependency `{from}` -> `{top}`; \
                     allowed targets for `{from}` are {:?} (see ARCHITECTURE.md)",
                    relative.display(),
                    allowed.get(from.as_str()).cloned().unwrap_or_default()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "architecture boundary violations:\n{}\n\n\
         These rules mirror ARCHITECTURE.md. Either move the code to respect \
         the boundary or change the architecture deliberately: update \
         ARCHITECTURE.md and this test in the same change.",
        violations.join("\n")
    );
}

#[test]
fn external_crates_stay_inside_their_owning_modules() {
    let src = source_root();
    let files = rust_sources(&src);
    let containment = external_crate_containment();

    let mut violations = Vec::new();
    for file in &files {
        let relative = file.strip_prefix(&src).unwrap();
        let relative_text = relative.to_string_lossy().replace('\\', "/");
        let text = read_source(file);

        for (crate_name, allowed_prefixes) in &containment {
            if allowed_prefixes
                .iter()
                .any(|prefix| relative_text == *prefix || relative_text.starts_with(prefix))
            {
                continue;
            }
            for (line_number, line) in text.lines().enumerate() {
                if references_external_crate(strip_line_comment(line), crate_name) {
                    violations.push(format!(
                        "{relative_text}:{}: `{crate_name}` is confined to \
                         {allowed_prefixes:?} (see ARCHITECTURE.md)",
                        line_number + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "external crate containment violations:\n{}\n\n\
         These rules mirror ARCHITECTURE.md. Either move the code into the \
         owning module or widen the containment deliberately: update \
         ARCHITECTURE.md and this test in the same change.",
        violations.join("\n")
    );
}

fn source_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|error| panic!("read {}: {error}", directory.display()));
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn read_source(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn top_level_modules(src: &Path) -> BTreeSet<String> {
    let mut modules = BTreeSet::new();
    let entries =
        fs::read_dir(src).unwrap_or_else(|error| panic!("read {}: {error}", src.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("read {}: {error}", src.display()));
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            modules.insert(name);
        } else if let Some(stem) = name.strip_suffix(".rs") {
            modules.insert(stem.to_string());
        }
    }
    modules
}

/// Logical module path of a file relative to `src/`, e.g.
/// `runner/enrollment/tests.rs` -> `["runner", "enrollment", "tests"]` and
/// `api/mod.rs` -> `["api"]`. `main.rs` maps to the empty path.
fn module_path_of(relative: &Path) -> Vec<String> {
    let mut segments: Vec<String> = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect();
    let file = segments.pop().unwrap();
    match file.strip_suffix(".rs") {
        Some("main") if segments.is_empty() => {}
        Some("mod") => {}
        Some(stem) => segments.push(stem.to_string()),
        None => {}
    }
    segments
}

/// Crate-internal targets referenced by a file: `crate::a::b` yields
/// `a::b`, and a `super::` chain that escapes the file's top-level module
/// yields the referenced top-level module name.
fn referenced_targets(
    text: &str,
    module_path: &[String],
    top_modules: &BTreeSet<String>,
) -> Vec<(usize, String)> {
    let mut targets = Vec::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line = strip_line_comment(raw_line);
        let line_number = index + 1;
        for target in crate_path_targets(line) {
            targets.push((line_number, target));
        }
        for target in escaping_super_targets(line, module_path.len(), top_modules) {
            targets.push((line_number, target));
        }
    }
    targets
}

/// Removes a `//` comment when it starts the line or follows whitespace, so
/// `://` inside string literals survives while doc and line comments do not.
fn strip_line_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    for index in 0..bytes.len().saturating_sub(1) {
        if bytes[index] == b'/'
            && bytes[index + 1] == b'/'
            && (index == 0 || bytes[index - 1].is_ascii_whitespace())
        {
            return &line[..index];
        }
    }
    line
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Extracts up to the first two segments of every `crate::...` path on the
/// line, joined with `::`.
fn crate_path_targets(line: &str) -> Vec<String> {
    const MARKER: &str = "crate::";
    let bytes = line.as_bytes();
    let mut targets = Vec::new();
    let mut search_from = 0;
    while let Some(found) = line[search_from..].find(MARKER) {
        let start = search_from + found;
        search_from = start + MARKER.len();
        if start > 0 {
            let before = bytes[start - 1];
            if is_ident_byte(before) || before == b':' {
                continue;
            }
        }
        let mut segments = Vec::new();
        let mut cursor = start + MARKER.len();
        while segments.len() < 2 {
            let segment_start = cursor;
            while cursor < bytes.len() && is_ident_byte(bytes[cursor]) {
                cursor += 1;
            }
            if cursor == segment_start {
                break;
            }
            segments.push(&line[segment_start..cursor]);
            if line[cursor..].starts_with("::") {
                cursor += 2;
            } else {
                break;
            }
        }
        if let Some(first) = segments.first()
            && first.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        {
            targets.push(segments.join("::"));
        }
    }
    targets
}

/// Resolves `super::` chains that climb past the file's top-level module to
/// a crate-root sibling. Only identifiers that name real top-level modules
/// count, which keeps items inside inline `#[cfg(test)]` modules (whose
/// extra nesting this lexical scan cannot see) from producing false edges.
fn escaping_super_targets(
    line: &str,
    module_depth: usize,
    top_modules: &BTreeSet<String>,
) -> Vec<String> {
    const MARKER: &str = "super::";
    let bytes = line.as_bytes();
    let mut targets = Vec::new();
    let mut search_from = 0;
    while let Some(found) = line[search_from..].find(MARKER) {
        let start = search_from + found;
        if start > 0 {
            let before = bytes[start - 1];
            if is_ident_byte(before) || before == b':' {
                search_from = start + MARKER.len();
                continue;
            }
        }
        let mut supers = 0;
        let mut cursor = start;
        while line[cursor..].starts_with(MARKER) {
            supers += 1;
            cursor += MARKER.len();
        }
        search_from = cursor;
        if supers < module_depth {
            continue;
        }
        let segment_start = cursor;
        let mut segment_end = cursor;
        while segment_end < bytes.len() && is_ident_byte(bytes[segment_end]) {
            segment_end += 1;
        }
        let ident = &line[segment_start..segment_end];
        if top_modules.contains(ident) {
            targets.push(ident.to_string());
        }
    }
    targets
}

fn is_path_prefix(target: &str, prefix: &str) -> bool {
    target == prefix
        || target
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with("::"))
}

fn references_external_crate(line: &str, crate_name: &str) -> bool {
    let marker = format!("{crate_name}::");
    let bytes = line.as_bytes();
    let mut search_from = 0;
    while let Some(found) = line[search_from..].find(&marker) {
        let start = search_from + found;
        search_from = start + marker.len();
        if start > 0 {
            let before = bytes[start - 1];
            if is_ident_byte(before) || before == b':' {
                continue;
            }
        }
        return true;
    }
    false
}
