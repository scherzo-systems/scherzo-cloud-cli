//! Cargo-workspace and source-boundary architecture tests.
//!
//! Slice 1 has four unpublished leaf packages at their final roots. These tests
//! keep Cargo's package graph, the residual root-module graph, generated-source
//! privacy, and external-crate ownership aligned with `ARCHITECTURE.md`.

#![allow(
    clippy::disallowed_macros,
    reason = "the architecture test resolves paths from Cargo-provided package metadata"
)]
#![allow(
    clippy::expect_used,
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
use std::process::Command;

use serde_json::Value;

const INTERNAL_PACKAGES: [&str; 5] = [
    "scherzo-cloud",
    "scherzo-cloud-api",
    "scherzo-cloud-runner-protocol",
    "scherzo-cloud-support",
    "scherzo-cloud-test-support",
];

#[test]
fn workspace_members_and_edges_match_slice_one() {
    let root = cli_root();
    let metadata = cargo_metadata(&root);
    let packages = metadata["packages"]
        .as_array()
        .expect("Cargo metadata packages should be an array");
    let workspace_ids = metadata["workspace_members"]
        .as_array()
        .expect("Cargo metadata workspace_members should be an array")
        .iter()
        .map(|value| value.as_str().expect("workspace member ID should be text"))
        .collect::<BTreeSet<_>>();

    let expected_manifests = BTreeMap::from([
        ("scherzo-cloud", "Cargo.toml"),
        ("scherzo-cloud-api", "crates/api/Cargo.toml"),
        (
            "scherzo-cloud-runner-protocol",
            "crates/runner-protocol/Cargo.toml",
        ),
        ("scherzo-cloud-support", "crates/support/Cargo.toml"),
        (
            "scherzo-cloud-test-support",
            "crates/test-support/Cargo.toml",
        ),
    ]);
    let workspace_packages = packages
        .iter()
        .filter(|package| {
            workspace_ids.contains(
                package["id"]
                    .as_str()
                    .expect("Cargo package ID should be text"),
            )
        })
        .map(|package| {
            (
                package["name"]
                    .as_str()
                    .expect("Cargo package name should be text"),
                package,
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        workspace_packages.keys().copied().collect::<BTreeSet<_>>(),
        INTERNAL_PACKAGES.into_iter().collect(),
        "Slice 1 must contain exactly the root package and four final leaf members"
    );

    for (name, relative_manifest) in expected_manifests {
        let package = workspace_packages
            .get(name)
            .unwrap_or_else(|| panic!("missing workspace package {name}"));
        let manifest = Path::new(
            package["manifest_path"]
                .as_str()
                .expect("manifest path should be text"),
        );
        assert_eq!(
            manifest,
            root.join(relative_manifest),
            "{name} is not rooted at its final Slice 1 path"
        );
        assert_eq!(
            package["publish"].as_array().map(Vec::len),
            Some(0),
            "{name} must remain unpublished"
        );
        let manifest_text = read_source(manifest);
        assert!(
            manifest_text.contains("[lints]\nworkspace = true"),
            "{name} must inherit workspace lints"
        );
    }

    let internal_names = INTERNAL_PACKAGES.into_iter().collect::<BTreeSet<_>>();
    let mut actual_edges = BTreeSet::new();
    for (from, package) in &workspace_packages {
        for dependency in package["dependencies"]
            .as_array()
            .expect("Cargo dependencies should be an array")
        {
            let to = dependency["name"]
                .as_str()
                .expect("dependency name should be text");
            if !internal_names.contains(to) {
                continue;
            }
            let kind = dependency["kind"].as_str().unwrap_or("normal");
            actual_edges.insert((*from, to, kind));
        }
    }
    let expected_edges = BTreeSet::from([
        ("scherzo-cloud", "scherzo-cloud-api", "normal"),
        ("scherzo-cloud", "scherzo-cloud-runner-protocol", "normal"),
        ("scherzo-cloud", "scherzo-cloud-support", "normal"),
        ("scherzo-cloud", "scherzo-cloud-test-support", "dev"),
        ("scherzo-cloud-api", "scherzo-cloud-support", "normal"),
        ("scherzo-cloud-api", "scherzo-cloud-test-support", "dev"),
    ]);
    assert_eq!(
        actual_edges, expected_edges,
        "unexpected internal Cargo edge"
    );
}

#[test]
fn moved_sources_have_one_final_owner_and_private_generated_api() {
    let root = cli_root();
    for obsolete in [
        "src/api",
        "src/runner_protocol",
        "src/public_id.rs",
        "src/timing.rs",
        "src/tls.rs",
        "src/workflow_contract.rs",
        "src/workflow_contract",
    ] {
        assert!(
            !root.join(obsolete).exists(),
            "moved Slice 1 source remains at obsolete path {obsolete}"
        );
    }

    let api_facade = read_source(&root.join("crates/api/src/lib.rs"));
    assert!(api_facade.contains("mod generated;"));
    assert!(!api_facade.contains("pub mod generated;"));

    for facade in [
        "crates/api/src/lib.rs",
        "crates/runner-protocol/src/lib.rs",
        "crates/support/src/lib.rs",
    ] {
        let text = read_source(&root.join(facade));
        assert!(
            !text
                .lines()
                .any(|line| line.trim_start().starts_with("pub mod ")),
            "{facade} exposes an implementation module instead of an explicit facade"
        );
    }

    let mut violations = Vec::new();
    for source in all_package_sources(&root) {
        let relative = source.strip_prefix(&root).unwrap();
        let text = read_source(&source);
        if text.contains("scherzo_cloud_api::generated") {
            violations.push(format!(
                "{} names the private generated API module",
                relative.display()
            ));
        }
        if text.contains("crate::generated") && !relative.starts_with("crates/api/src") {
            violations.push(format!(
                "{} contains a generated API reference outside the API package",
                relative.display()
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "generated API boundary violations:\n{}",
        violations.join("\n")
    );
}

/// Residual root-module dependency allowlist. A module may always reference
/// itself; `main.rs` remains the composition root and is not constrained.
fn allowed_dependencies() -> BTreeMap<&'static str, BTreeSet<&'static str>> {
    let entries: &[(&str, &[&str])] = &[
        ("build_info", &[]),
        (
            "cli",
            &[
                "build_info",
                "execution",
                "exit_code",
                "human_auth",
                "idempotency",
                "runner",
                "service_auth",
            ],
        ),
        ("error", &["exit_code"]),
        ("execution", &["build_info", "exit_code", "process"]),
        ("exit_code", &[]),
        ("human_auth", &[]),
        ("idempotency", &[]),
        ("process", &[]),
        (
            "runner",
            &["build_info", "execution", "idempotency", "process"],
        ),
        ("service_auth", &[]),
        ("test_support", &[]),
    ];
    entries
        .iter()
        .map(|(from, to)| (*from, to.iter().copied().collect()))
        .collect()
}

fn special_targets() -> Vec<(&'static str, BTreeSet<&'static str>)> {
    vec![(
        "test_support",
        ["execution", "runner"].into_iter().collect(),
    )]
}

#[test]
fn residual_module_dependencies_match_architecture() {
    let src = cli_root().join("src");
    let files = rust_sources(&src);
    let top_modules = top_level_modules(&src);
    let allowed = allowed_dependencies();
    let specials = special_targets();
    let mut violations = Vec::new();

    for module in &top_modules {
        if module != "main" && !allowed.contains_key(module.as_str()) {
            violations.push(format!(
                "src has top-level module `{module}` with no residual allowlist entry"
            ));
        }
    }
    for (from, targets) in &allowed {
        for name in std::iter::once(from).chain(targets.iter()) {
            if !top_modules.contains(*name) {
                violations.push(format!("residual allowlist names absent module `{name}`"));
            }
        }
    }

    for file in &files {
        let relative = file.strip_prefix(&src).unwrap();
        let module_path = module_path_of(relative);
        let Some(from) = module_path.first().cloned() else {
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
                        "{}:{line_number}: `{from}` references `{prefix}`, reserved to {allowed_from:?}",
                        relative.display()
                    ));
                }
                continue;
            }
            if !allowed
                .get(from.as_str())
                .is_some_and(|targets| targets.contains(top))
            {
                violations.push(format!(
                    "{}:{line_number}: forbidden residual dependency `{from}` -> `{top}`",
                    relative.display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "architecture boundary violations:\n{}",
        violations.join("\n")
    );
}

/// External crates and the package-owned source prefixes that may use them.
fn external_crate_containment() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        ("clap", vec!["src/cli.rs", "src/cli/"]),
        (
            "reqwest",
            vec!["crates/api/src/", "src/human_auth/", "src/runner/"],
        ),
        ("tokio_tungstenite", vec!["src/runner/service/"]),
        ("opentelemetry", vec!["src/runner/"]),
        ("opentelemetry_sdk", vec!["src/runner/"]),
        ("opentelemetry_proto", vec!["src/runner/"]),
        ("ratatui", vec!["src/execution/workflow/"]),
        ("crossterm", vec!["src/execution/workflow/"]),
    ]
}

#[test]
fn external_crates_stay_inside_their_owning_packages() {
    let root = cli_root();
    let containment = external_crate_containment();
    let mut violations = Vec::new();

    for file in all_package_sources(&root) {
        let relative = file.strip_prefix(&root).unwrap();
        let relative_text = relative.to_string_lossy().replace('\\', "/");
        let text = read_source(&file);
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
                        "{relative_text}:{}: `{crate_name}` is confined to {allowed_prefixes:?}",
                        line_number + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "external crate containment violations:\n{}",
        violations.join("\n")
    );
}

fn cli_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn cargo_metadata(root: &Path) -> Value {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--locked",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(root.join("Cargo.toml"))
        .output()
        .expect("Cargo metadata should start");
    assert!(
        output.status.success(),
        "Cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("Cargo metadata should be JSON")
}

fn all_package_sources(root: &Path) -> Vec<PathBuf> {
    let mut files = rust_sources(&root.join("src"));
    let crates = root.join("crates");
    for member in fs::read_dir(&crates).expect("read crates directory") {
        let member = member.expect("read crates member");
        files.extend(rust_sources(&member.path().join("src")));
    }
    files.sort();
    files
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
            && first
                .chars()
                .next()
                .is_some_and(|character| character.is_ascii_lowercase())
        {
            targets.push(segments.join("::"));
        }
    }
    targets
}

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
