//! Cargo-workspace and source-boundary architecture tests.
//!
//! Slice 3 gives runner behavior and idempotency their final package owners.
//! These tests keep Cargo's package graph, residual root modules, explicit
//! component facades, generated-source privacy, and external-crate ownership
//! aligned with `ARCHITECTURE.md`.

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

const INTERNAL_PACKAGES: [&str; 7] = [
    "scherzo-cloud",
    "scherzo-cloud-api",
    "scherzo-cloud-execution",
    "scherzo-cloud-runner",
    "scherzo-cloud-runner-protocol",
    "scherzo-cloud-support",
    "scherzo-cloud-test-support",
];

#[test]
fn workspace_members_and_edges_match_slice_three() {
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
        ("scherzo-cloud-execution", "crates/execution/Cargo.toml"),
        ("scherzo-cloud-runner", "crates/runner/Cargo.toml"),
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
        "Slice 3 must contain the root package and six final component members"
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
            "{name} is not rooted at its final package path"
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
        ("scherzo-cloud", "scherzo-cloud-execution", "normal"),
        ("scherzo-cloud", "scherzo-cloud-execution", "dev"),
        ("scherzo-cloud", "scherzo-cloud-runner", "normal"),
        ("scherzo-cloud", "scherzo-cloud-runner", "dev"),
        ("scherzo-cloud", "scherzo-cloud-support", "normal"),
        ("scherzo-cloud", "scherzo-cloud-test-support", "dev"),
        ("scherzo-cloud-api", "scherzo-cloud-support", "normal"),
        ("scherzo-cloud-api", "scherzo-cloud-test-support", "dev"),
        ("scherzo-cloud-execution", "scherzo-cloud-support", "normal"),
        (
            "scherzo-cloud-execution",
            "scherzo-cloud-test-support",
            "dev",
        ),
        ("scherzo-cloud-runner", "scherzo-cloud-execution", "normal"),
        ("scherzo-cloud-runner", "scherzo-cloud-execution", "dev"),
        (
            "scherzo-cloud-runner",
            "scherzo-cloud-runner-protocol",
            "normal",
        ),
        ("scherzo-cloud-runner", "scherzo-cloud-support", "normal"),
        ("scherzo-cloud-runner", "scherzo-cloud-test-support", "dev"),
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
        "src/execution",
        "src/process.rs",
        "src/runner",
        "src/idempotency.rs",
        "src/test_support.rs",
        "tests/liv_2331_remediation_repro.rs",
        "tests/fixtures/codex-app-server-v1-schema",
        "tests/fixtures/workflow/v1",
    ] {
        assert!(
            !root.join(obsolete).exists(),
            "moved source remains at obsolete path {obsolete}"
        );
    }
    for owned in [
        "crates/execution/tests/fixtures/codex-app-server-v1-schema",
        "crates/execution/tests/fixtures/workflow/v1",
    ] {
        assert!(
            root.join(owned).is_dir(),
            "execution-owned fixture is absent from {owned}"
        );
    }
    assert!(
        root.join("crates/execution/examples/internal-worker.rs")
            .is_file(),
        "execution package test worker is absent"
    );

    let api_facade = read_source(&root.join("crates/api/src/lib.rs"));
    assert!(api_facade.contains("mod generated;"));
    assert!(!api_facade.contains("pub mod generated;"));

    let support_facade = read_source(&root.join("crates/support/src/lib.rs"));
    assert!(support_facade.contains("mod idempotency;"));
    assert!(support_facade.contains("pub use idempotency::generate_idempotency_key;"));

    let runner_facade = read_source(&root.join("crates/runner/src/lib.rs"));
    for implementation in [
        "control_client",
        "control_protocol",
        "credential",
        "doctor",
        "enrollment",
        "service",
        "telemetry",
        "validation",
    ] {
        assert!(
            runner_facade.contains(&format!("mod {implementation};")),
            "runner facade does not privately own {implementation}"
        );
    }
    for consumed in [
        "pub use control_client::{RequestFailure, request};",
        "pub use service::{Config, ConfigError};",
        "pub struct ServiceError",
        "pub fn run(config: Config, service_version: &str)",
    ] {
        assert!(
            runner_facade.contains(consumed),
            "runner facade is missing command/helper seam `{consumed}`"
        );
    }

    for facade in [
        "crates/api/src/lib.rs",
        "crates/execution/src/lib.rs",
        "crates/runner/src/lib.rs",
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
            &["build_info", "exit_code", "human_auth", "service_auth"],
        ),
        ("error", &["exit_code"]),
        ("exit_code", &[]),
        ("human_auth", &[]),
        ("service_auth", &[]),
    ];
    entries
        .iter()
        .map(|(from, to)| (*from, to.iter().copied().collect()))
        .collect()
}

fn special_targets() -> Vec<(&'static str, BTreeSet<&'static str>)> {
    Vec::new()
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

const EXECUTION_IMPLEMENTATION_MODULES: [&str; 6] = [
    "claude_code",
    "codex",
    "harness_installation",
    "owned_tree",
    "pi",
    "workflow",
];

#[test]
fn execution_facade_and_process_ownership_match_slice_two_b() {
    let root = cli_root();
    let facade_path = root.join("crates/execution/src/lib.rs");
    let facade = read_source(&facade_path);
    assert!(exposed_module_declarations(&facade).is_empty());
    assert!(facade.contains("mod process;"));

    let process_export = facade
        .split_once("pub use process::{")
        .and_then(|(_, suffix)| suffix.split_once("};"))
        .map(|(body, _)| body)
        .expect("execution facade should export its process inventory");
    let actual_process_exports = source_tokens(process_export)
        .into_iter()
        .filter(|token| is_source_identifier(token.text))
        .map(|token| token.text)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_process_exports,
        BTreeSet::from([
            "CommandOutput",
            "CommandProbeError",
            "CommandRequest",
            "CommandRunner",
            "ManagedProcessGroup",
            "SystemCommandRunner",
        ]),
        "execution must expose exactly the six inventoried process interfaces",
    );

    let mut violations = Vec::new();
    for source in rust_sources(&root.join("src")) {
        let relative = source.strip_prefix(&root).unwrap();
        let text = read_source(&source);
        for obsolete in ["crate::execution", "crate::process"] {
            if text.contains(obsolete) {
                violations.push(format!(
                    "{} retains obsolete root path `{obsolete}`",
                    relative.display()
                ));
            }
        }
        for module in EXECUTION_IMPLEMENTATION_MODULES {
            let deep_path = format!("scherzo_cloud_execution::{module}");
            if text.contains(&deep_path) {
                violations.push(format!(
                    "{} names private execution module `{module}`",
                    relative.display()
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "execution ownership violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn execution_boundary_policy_rejects_public_modules() {
    for exposed_module in [
        "pub mod workflow;\n",
        "pub(crate) mod workflow;\n",
        "pub(super) mod workflow;\n",
        "pub(in crate) mod workflow;\n",
    ] {
        assert!(
            !exposed_module_declarations(exposed_module).is_empty(),
            "accepted exposed execution module: {exposed_module}"
        );
    }
    assert!(exposed_module_declarations("mod workflow;\n").is_empty());
}

#[test]
fn execution_and_runner_receive_identity_without_reading_root_build_policy() {
    let root = cli_root();
    let mut violations = Vec::new();
    for owner in ["crates/execution/src", "crates/runner/src"] {
        for source in rust_sources(&root.join(owner)) {
            let relative = source.strip_prefix(&root).unwrap();
            let text = read_source(&source);
            for forbidden in [
                "crate::build_info",
                "crate::exit_code",
                "SCHERZO_CLOUD_VERSION",
                "SCHERZO_CLOUD_BUILD_IDENTITY",
                "CARGO_PKG_VERSION",
            ] {
                if text.contains(forbidden) {
                    violations.push(format!(
                        "{} reads root identity or exit policy through `{forbidden}`",
                        relative.display()
                    ));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "root policy leaked into execution or runner:\n{}",
        violations.join("\n")
    );
}

#[derive(Clone, Copy)]
struct SourceToken<'a> {
    text: &'a str,
    line_number: usize,
}

fn source_tokens(source: &str) -> Vec<SourceToken<'_>> {
    let mut tokens = Vec::new();
    for (index, raw_line) in source.lines().enumerate() {
        let line = strip_line_comment(raw_line);
        let bytes = line.as_bytes();
        let mut cursor = 0;
        while cursor < bytes.len() {
            if is_ident_byte(bytes[cursor]) {
                let start = cursor;
                cursor += 1;
                while cursor < bytes.len() && is_ident_byte(bytes[cursor]) {
                    cursor += 1;
                }
                tokens.push(SourceToken {
                    text: &line[start..cursor],
                    line_number: index + 1,
                });
            } else if bytes[cursor] == b':'
                && bytes.get(cursor + 1).is_some_and(|byte| *byte == b':')
            {
                tokens.push(SourceToken {
                    text: "::",
                    line_number: index + 1,
                });
                cursor += 2;
            } else {
                if matches!(bytes[cursor], b'{' | b'}' | b'(' | b')' | b',') {
                    tokens.push(SourceToken {
                        text: &line[cursor..cursor + 1],
                        line_number: index + 1,
                    });
                }
                cursor += 1;
            }
        }
    }
    tokens
}

fn rooted_module_references<'a>(source: &'a str, root: &str) -> Vec<(usize, &'a str)> {
    let tokens = source_tokens(source);
    let mut references = Vec::new();
    for index in 0..tokens.len() {
        if tokens[index].text == root
            && tokens
                .get(index + 1)
                .is_some_and(|token| token.text == "::")
        {
            references.extend(module_references_at(&tokens, index + 2));
        }
    }
    references
}

fn escaping_super_module_references(source: &str, module_depth: usize) -> Vec<(usize, &str)> {
    let tokens = source_tokens(source);
    let mut references = Vec::new();
    for index in 0..tokens.len() {
        if tokens[index].text != "super"
            || index
                .checked_sub(1)
                .and_then(|previous| tokens.get(previous))
                .is_some_and(|token| token.text == "::")
        {
            continue;
        }
        let mut supers = 0_usize;
        let mut cursor = index;
        while tokens
            .get(cursor)
            .is_some_and(|token| token.text == "super")
            && tokens
                .get(cursor + 1)
                .is_some_and(|token| token.text == "::")
        {
            supers += 1;
            cursor += 2;
        }
        if supers >= module_depth {
            references.extend(module_references_at(&tokens, cursor));
        }
    }
    references
}

fn module_references_at<'a>(tokens: &[SourceToken<'a>], index: usize) -> Vec<(usize, &'a str)> {
    module_token_indexes_at(tokens, index)
        .into_iter()
        .map(|index| (tokens[index].line_number, tokens[index].text))
        .collect()
}

fn module_token_indexes_at(tokens: &[SourceToken<'_>], index: usize) -> Vec<usize> {
    let Some(next) = tokens.get(index) else {
        return Vec::new();
    };
    if next.text != "{" {
        return if is_source_identifier(next.text) && next.text != "self" {
            vec![index]
        } else {
            Vec::new()
        };
    }

    let mut indexes = Vec::new();
    let mut depth = 1_usize;
    let mut entry_start = true;
    let mut cursor = index + 1;
    while let Some(token) = tokens.get(cursor) {
        if depth == 1 && entry_start && is_source_identifier(token.text) {
            if token.text != "self" {
                indexes.push(cursor);
            }
            entry_start = false;
        }
        match token.text {
            "{" => depth += 1,
            "}" => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            "," if depth == 1 => entry_start = true,
            _ => {}
        }
        cursor += 1;
    }
    indexes
}

fn is_source_identifier(token: &str) -> bool {
    token
        .as_bytes()
        .first()
        .is_some_and(|byte| is_ident_byte(*byte))
}

fn exposed_module_declarations(source: &str) -> Vec<usize> {
    let tokens = source_tokens(source);
    let mut declarations = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.text != "pub" {
            continue;
        }
        let mut cursor = index + 1;
        if tokens.get(cursor).is_some_and(|token| token.text == "(") {
            let mut depth = 1_usize;
            cursor += 1;
            while let Some(token) = tokens.get(cursor) {
                match token.text {
                    "(" => depth += 1,
                    ")" => {
                        depth -= 1;
                        if depth == 0 {
                            cursor += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                cursor += 1;
            }
        }
        if tokens.get(cursor).is_some_and(|token| token.text == "mod") {
            declarations.push(token.line_number);
        }
    }
    declarations
}

/// External crates and the package-owned source prefixes that may use them.
fn external_crate_containment() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        ("clap", vec!["src/cli.rs", "src/cli/"]),
        (
            "reqwest",
            vec!["crates/api/src/", "src/human_auth/", "crates/runner/src/"],
        ),
        (
            "tokio_tungstenite",
            vec!["crates/runner/src/service/", "tests/cli/runner_status.rs"],
        ),
        ("opentelemetry", vec!["crates/runner/src/"]),
        ("opentelemetry_sdk", vec!["crates/runner/src/"]),
        ("opentelemetry_proto", vec!["crates/runner/src/"]),
        ("ratatui", vec!["crates/execution/src/workflow/"]),
        ("crossterm", vec!["crates/execution/src/workflow/"]),
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
    rooted_module_references(text, "crate")
        .into_iter()
        .chain(escaping_super_module_references(text, module_path.len()))
        .filter(|(_, module)| top_modules.contains(*module))
        .map(|(line_number, module)| (line_number, module.to_owned()))
        .collect()
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
