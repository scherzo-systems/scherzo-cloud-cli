//! Structural CLI grammar ratchet. Paths, not rendered help, identify debt.
use std::collections::{BTreeMap, BTreeSet};

use clap::{Arg, ArgAction, Command, CommandFactory};

use super::Cli;

const LEAVES: &str = "accept begin cancel complete continue create decline delete disable doctor download drain enable end enroll history issue leave link list login logout move preview propose reference remove rename request retire retry revoke run schema seal serve set show signup status update upload validate version view wait";
const GROUPS: &str = "account activation artifact audit auth authorization connection credential delegation deletion github identity input input-set installation invitation linear member organization pool project publication repository run runner runner-pool service-principal setup workflow";
const COMMON: [&str; 3] = ["json", "service-api-key-file", "allow-insecure-http"];

// These identities are backed by shared Args or identifier argument types in cli/src/cli.rs
// and cli/src/cli/entity.rs. A long spelling alone is never a help identity.
fn semantic_class(path: &str, long: &str) -> Option<String> {
    let shared = match long {
        "service-api-key-file"
        | "allow-insecure-http"
        | "cursor"
        | "organization"
        | "pool"
        | "project-id"
        | "installation-id"
        | "repository-id"
        | "timeout"
        | "yes"
        | "input-text"
        | "input-text-file"
        | "input-json"
        | "input-json-file"
        | "input-file"
        | "input-attachment"
        | "input-attachments-empty" => long,
        // Only these paths flatten PaginationArgs. Other options named --limit
        // do not acquire pagination semantics just by sharing the spelling.
        "limit" => match path {
            "organization audit list" => "limit:100",
            "artifact list"
            | "auth identity list"
            | "delegation list"
            | "invitation list"
            | "organization list"
            | "organization invitation list"
            | "organization member list"
            | "organization member history"
            | "project list"
            | "publication list"
            | "runner pool list"
            | "runner activation list"
            | "runner credential list"
            | "runner list"
            | "service-principal credential list" => "limit:200",
            _ => return None,
        },
        "json" => {
            // JsonArgs<F> is parameterized by output family; deletion cancellation
            // uses StreamingJson rather than DeletionJson.
            let family = if matches!(
                path,
                "auth login"
                    | "auth identity link"
                    | "account deletion cancel"
                    | "organization deletion cancel"
            ) {
                "streaming-events"
            } else if matches!(path, "auth logout" | "auth status") {
                "sign-in"
            } else if path.starts_with("auth identity ") {
                "identity"
            } else if path.starts_with("organization invitation ") {
                "invitation"
            } else if path.starts_with("account deletion ")
                || path.starts_with("organization deletion ")
            {
                "deletion"
            } else {
                path.split(' ').next().unwrap_or("")
            };
            return Some(format!("json:{family}"));
        }
        _ => return None,
    };
    Some(shared.to_owned())
}

fn value_shape(arg: &Arg) -> String {
    if matches!(
        arg.get_action(),
        ArgAction::SetTrue
            | ArgAction::SetFalse
            | ArgAction::Count
            | ArgAction::Help
            | ArgAction::Version
    ) {
        return "flag".to_owned();
    }
    // clap uses the ID verbatim when no value name is supplied. The value
    // count is part of the shape even when the printed names are unchanged.
    let names = arg.get_value_names().map_or_else(
        || vec![arg.get_id().to_string()],
        |names| names.iter().map(ToString::to_string).collect(),
    );
    let count = arg.get_num_args().unwrap_or(1.into());
    format!(
        "value:{}:{}..={}",
        names.join(","),
        count.min_values(),
        count.max_values()
    )
}

fn violations(root: &Command) -> BTreeSet<String> {
    let mut failures = BTreeSet::new();
    let mut shapes: BTreeMap<String, (String, String)> = BTreeMap::new();
    let mut helps: BTreeMap<String, (String, String)> = BTreeMap::new();
    let mut descriptions: BTreeMap<String, String> = BTreeMap::new();

    fn visit(
        node: &Command,
        path: &str,
        failures: &mut BTreeSet<String>,
        shapes: &mut BTreeMap<String, (String, String)>,
        helps: &mut BTreeMap<String, (String, String)>,
        descriptions: &mut BTreeMap<String, String>,
    ) {
        let children: Vec<_> = node
            .get_subcommands()
            .filter(|c| c.get_name() != "help")
            .collect();
        if !path.is_empty() {
            let token = node.get_name();
            if children.is_empty() {
                if !LEAVES.split_whitespace().any(|verb| verb == token) {
                    failures.insert(format!("leaf-token|{path}"));
                }
                let destructive = matches!(
                    token,
                    "delete" | "decline" | "end" | "leave" | "remove" | "revoke" | "retire"
                ) || matches!(
                    path,
                    "account deletion request"
                        | "organization deletion request"
                        | "github installation disconnect"
                        | "project repository detach"
                );
                let has = |long| node.get_arguments().any(|arg| arg.get_long() == Some(long));
                if destructive
                    && !node.get_arguments().any(|arg| {
                        arg.get_long() == Some("yes")
                            && matches!(arg.get_action(), ArgAction::SetTrue)
                            && arg.is_required_set()
                    })
                {
                    failures.insert(format!("confirmation|{path}"));
                }
                if has("cursor")
                    && (!has("limit")
                        || !node.get_after_help().is_some_and(|note| {
                            note.to_string()
                                .matches(super::PAGINATION_AFTER_HELP)
                                .count()
                                == 1
                        }))
                {
                    failures.insert(format!("pagination|{path}"));
                }
                if let Some(description) = node.get_about() {
                    let description = description.to_string();
                    if let Some(other) = descriptions.insert(description.clone(), path.to_owned()) {
                        failures.insert(format!("description|{other}|{path}"));
                    }
                }
            } else if !GROUPS.split_whitespace().any(|noun| noun == token) {
                failures.insert(format!("group-token|{path}"));
            }
        }

        // clap sorts options by display order and then short or long spelling.
        // get_arguments() itself retains insertion order, which need not match help.
        let mut common: Vec<_> = node
            .get_arguments()
            .filter(|arg| arg.get_long().is_some_and(|long| COMMON.contains(&long)))
            .collect();
        common.sort_by_key(|arg| {
            let spelling = if let Some(short) = arg.get_short() {
                format!(
                    "{}{}",
                    short.to_ascii_lowercase(),
                    if short.is_ascii_lowercase() { '0' } else { '1' }
                )
            } else {
                arg.get_long().unwrap_or_default().to_owned()
            };
            (arg.get_display_order(), spelling)
        });
        if common.windows(2).any(|pair| {
            let first = COMMON
                .iter()
                .position(|name| Some(*name) == pair[0].get_long());
            let second = COMMON
                .iter()
                .position(|name| Some(*name) == pair[1].get_long());
            first >= second
        }) {
            failures.insert(format!("common-order|{path}"));
        }
        for arg in node.get_arguments() {
            let Some(long) = arg.get_long() else { continue };
            let shape = value_shape(arg);
            if let Some((other, expected)) = shapes.get(long) {
                if expected != &shape {
                    failures.insert(format!("placeholder|--{long}|{other}|{path}"));
                }
            } else {
                shapes.insert(long.to_owned(), (path.to_owned(), shape));
            }
            if let Some(class) = semantic_class(path, long) {
                let help = arg.get_help().map(ToString::to_string).unwrap_or_default();
                if let Some((other, expected)) = helps.get(&class) {
                    if expected != &help {
                        failures.insert(format!("option-help|{class}|{other}|{path}"));
                    }
                } else {
                    helps.insert(class, (path.to_owned(), help));
                }
            }
        }
        for child in children {
            let child_path = if path.is_empty() {
                child.get_name().to_owned()
            } else {
                format!("{path} {}", child.get_name())
            };
            visit(child, &child_path, failures, shapes, helps, descriptions);
        }
    }
    visit(
        root,
        "",
        &mut failures,
        &mut shapes,
        &mut helps,
        &mut descriptions,
    );
    failures
}

// A ratchet: entries must be unique, sorted, and observed. Each entry has one burn-down owner.
const BASELINE: &[(&str, &str)] = &[
    (
        "description|github installation list|project repository installation list",
        "LIV-2428",
    ),
    ("leaf-token|github installation disconnect", "LIV-2426"),
    ("leaf-token|project repository detach", "LIV-2426"),
    (
        "placeholder|--display-name|account update|organization create",
        "LIV-2422",
    ),
    (
        "placeholder|--display-name|account update|organization update",
        "LIV-2422",
    ),
    (
        "placeholder|--display-name|account update|run create",
        "LIV-2422",
    ),
    (
        "placeholder|--output|artifact download|run input download",
        "LIV-2422",
    ),
];

fn check_baseline(observed: &BTreeSet<String>, baseline: &[(&str, &str)]) -> Result<(), String> {
    let mut allowed = BTreeSet::new();
    let mut previous = None;
    for &(id, owner) in baseline {
        if previous.is_some_and(|last| last >= id) || !allowed.insert(id.to_owned()) {
            return Err(format!("unordered-or-duplicate|{id}"));
        }
        let expected_owner = if id.starts_with("placeholder|") {
            "LIV-2422"
        } else if id.starts_with("leaf-token|") {
            "LIV-2426"
        } else if id.starts_with("confirmation|") || id.starts_with("option-help|yes|") {
            "LIV-2427"
        } else if id.starts_with("description|") {
            "LIV-2428"
        } else {
            return Err(format!("unmapped-category|{id}"));
        };
        if owner != expected_owner {
            return Err(format!("wrong-owner|{id}|{owner}"));
        }
        previous = Some(id);
    }
    let unexpected: Vec<_> = observed.difference(&allowed).collect();
    let stale: Vec<_> = allowed.difference(observed).collect();
    if unexpected.is_empty() && stale.is_empty() {
        Ok(())
    } else {
        Err(format!("unexpected: {unexpected:?}\nstale: {stale:?}"))
    }
}

#[test]
fn cli_grammar_conforms_with_mapped_debt() {
    let mut root = Cli::command();
    root.build();
    let observed = violations(&root);
    assert!(
        check_baseline(&observed, BASELINE).is_ok(),
        "{}",
        check_baseline(&observed, BASELINE).unwrap_err()
    );
}

#[test]
fn destructive_leaves_require_confirmation_but_runner_modes_do_not() {
    let gated: &[&[&str]] = &[
        &[
            "service-principal",
            "credential",
            "revoke",
            "crd_01k0z6r1w8f4jy2m7q9v3x5abc",
            "--service-api-key-file",
            "key",
        ],
        &[
            "runner",
            "credential",
            "revoke",
            "example",
            "runner",
            "credential",
        ],
        &[
            "runner",
            "credential",
            "retire",
            "example",
            "runner",
            "credential",
        ],
        &[
            "runner",
            "activation",
            "revoke",
            "example",
            "runner",
            "activation",
        ],
        &["auth", "identity", "remove", "identity"],
        &[
            "github",
            "installation",
            "disconnect",
            "example",
            "installation",
        ],
        &["project", "repository", "detach", "example", "project"],
        &["project", "runner-pool", "remove", "example", "project"],
    ];
    for &args in gated {
        let input = std::iter::once("scherzo-cloud").chain(args.iter().copied());
        assert_eq!(
            super::parse(input).unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument,
            "{args:?}"
        );
        let input = std::iter::once("scherzo-cloud")
            .chain(args.iter().copied())
            .chain(std::iter::once("--yes"));
        assert!(super::parse(input).is_ok(), "{args:?}");
    }
    for mode in ["disable", "drain"] {
        assert!(
            super::parse(["scherzo-cloud", "runner", mode, "example", "runner"]).is_ok(),
            "{mode}"
        );
    }
}

#[test]
fn structural_rules_reject_discriminating_changes() {
    let fixture = || {
        Command::new("fixture").subcommand(
            Command::new("entity")
                .subcommand(
                    Command::new("list")
                        .about("A")
                        .arg(Arg::new("cursor").long("cursor").help("same")),
                )
                .subcommand(Command::new("show").about("B")),
        )
    };
    let found = violations(&fixture());
    assert!(found.contains("pagination|entity list"));
    let complete = Command::new("fixture").subcommand(
        Command::new("entity").subcommand(
            Command::new("list")
                .about("A")
                .after_help(super::PAGINATION_AFTER_HELP)
                .arg(Arg::new("cursor").long("cursor"))
                .arg(Arg::new("limit").long("limit")),
        ),
    );
    assert!(
        !violations(&complete)
            .iter()
            .any(|id| id.starts_with("pagination|"))
    );
    let repeated_note = Command::new("fixture").subcommand(
        Command::new("entity").subcommand(
            Command::new("list")
                .about("A")
                .after_help(format!(
                    "{}\n{}",
                    super::PAGINATION_AFTER_HELP,
                    super::PAGINATION_AFTER_HELP
                ))
                .arg(Arg::new("cursor").long("cursor"))
                .arg(Arg::new("limit").long("limit")),
        ),
    );
    assert!(violations(&repeated_note).contains("pagination|entity list"));
    let found = violations(
        &Command::new("fixture")
            .subcommand(Command::new("people").subcommand(Command::new("detach").about("A"))),
    );
    assert!(found.contains("group-token|people"));
    assert!(found.contains("leaf-token|people detach"));
    let found = violations(
        &Command::new("fixture").subcommand(
            Command::new("entity")
                .subcommand(Command::new("remove").about("A"))
                .subcommand(Command::new("show").about("A")),
        ),
    );
    assert!(found.contains("confirmation|entity remove"));
    assert!(found.contains("description|entity remove|entity show"));
    let confirmation = |yes: Arg| {
        Command::new("fixture").subcommand(
            Command::new("entity").subcommand(Command::new("remove").about("A").arg(yes)),
        )
    };
    assert!(
        violations(&confirmation(
            Arg::new("yes").long("yes").action(ArgAction::SetTrue)
        ))
        .contains("confirmation|entity remove")
    );
    assert!(
        !violations(&confirmation(
            Arg::new("yes")
                .long("yes")
                .required(true)
                .action(ArgAction::SetTrue)
        ))
        .contains("confirmation|entity remove")
    );
    // The insertion order is correct; only display metadata reverses help order.
    let ordered = |http_order, json_order| {
        Command::new("fixture").subcommand(
            Command::new("entity").subcommand(
                Command::new("list")
                    .about("A")
                    .arg(
                        Arg::new("json")
                            .long("json")
                            .action(ArgAction::SetTrue)
                            .display_order(json_order),
                    )
                    .arg(
                        Arg::new("http")
                            .long("allow-insecure-http")
                            .action(ArgAction::SetTrue)
                            .display_order(http_order),
                    ),
            ),
        )
    };
    assert!(!violations(&ordered(2, 1)).contains("common-order|entity list"));
    assert!(violations(&ordered(0, 1)).contains("common-order|entity list"));
}

#[test]
fn option_identity_and_effective_placeholders_are_distinct() {
    let command = |second: Arg| {
        Command::new("fixture").subcommand(
            Command::new("account")
                .subcommand(
                    Command::new("show")
                        .about("A")
                        .arg(Arg::new("name").long("name").help("source")),
                )
                .subcommand(Command::new("list").about("B").arg(second)),
        )
    };
    // Same spelling with different semantic roles is not a help class.
    assert!(
        !violations(&command(Arg::new("name").long("name").help("destination")))
            .iter()
            .any(|id| id.starts_with("option-help"))
    );
    // Implicit placeholder <name> versus a valueless flag is not equivalent.
    assert!(
        violations(&command(
            Arg::new("name").long("name").action(ArgAction::SetTrue)
        ))
        .iter()
        .any(|id| id.starts_with("placeholder|--name"))
    );
    let shared = |second_help| {
        Command::new("fixture").subcommand(
            Command::new("account")
                .subcommand(
                    Command::new("show").about("A").arg(
                        Arg::new("json")
                            .long("json")
                            .action(ArgAction::SetTrue)
                            .help("first"),
                    ),
                )
                .subcommand(
                    Command::new("list").about("B").arg(
                        Arg::new("json")
                            .long("json")
                            .action(ArgAction::SetTrue)
                            .help(second_help),
                    ),
                ),
        )
    };
    assert!(
        violations(&shared("different"))
            .iter()
            .any(|id| id.starts_with("option-help|json:account"))
    );
    assert!(
        !violations(&shared("first"))
            .iter()
            .any(|id| id.starts_with("option-help"))
    );
    let placeholder = || {
        Command::new("fixture").subcommand(
            Command::new("account")
                .subcommand(
                    Command::new("show")
                        .about("A")
                        .arg(Arg::new("organization").long("organization").help("shared")),
                )
                .subcommand(
                    Command::new("list").about("B").arg(
                        Arg::new("organization")
                            .long("organization")
                            .value_name("OTHER")
                            .help("shared"),
                    ),
                ),
        )
    };
    assert!(
        violations(&placeholder())
            .iter()
            .any(|id| id.starts_with("placeholder|--organization"))
    );
    let implicit = Arg::new("name").long("name");
    assert_eq!(
        value_shape(&implicit),
        value_shape(&Arg::new("name").long("name").value_name("name"))
    );
    assert_ne!(
        value_shape(&implicit),
        value_shape(&Arg::new("name").long("name").value_name("NAME"))
    );
    assert!(
        !violations(&command(Arg::new("name").long("name").value_name("name")))
            .iter()
            .any(|id| id.starts_with("placeholder|--name"))
    );
    assert!(
        violations(&command(Arg::new("name").long("name").value_name("NAME")))
            .iter()
            .any(|id| id.starts_with("placeholder|--name"))
    );
    let cardinality = |count| command(Arg::new("name").long("name").num_args(count));
    assert!(
        violations(&cardinality(2))
            .iter()
            .any(|id| id.starts_with("placeholder|--name"))
    );
    assert!(
        !violations(&cardinality(1))
            .iter()
            .any(|id| id.starts_with("placeholder|--name"))
    );
    assert_ne!(
        value_shape(&Arg::new("name").long("name").num_args(1..=2)),
        value_shape(&Arg::new("name").long("name").num_args(1..=3))
    );
    // Both actual --project-id occurrences refer to ProjectArg; drift within
    // that class must fail even when another long option happens to match.
    let project = |second_help| {
        Command::new("fixture").subcommand(
            Command::new("run")
                .subcommand(
                    Command::new("create")
                        .about("A")
                        .arg(Arg::new("project_id").long("project-id").help("shared")),
                )
                .subcommand(
                    Command::new("input-set").subcommand(
                        Command::new("create")
                            .about("B")
                            .arg(Arg::new("project_id").long("project-id").help(second_help)),
                    ),
                ),
        )
    };
    assert!(
        !violations(&project("shared"))
            .iter()
            .any(|id| id.starts_with("option-help|project-id"))
    );
    assert!(
        violations(&project("drift"))
            .iter()
            .any(|id| id.starts_with("option-help|project-id|run create|run input-set create"))
    );
}

#[test]
fn parameterized_option_classes_are_isolated_but_ratchet_within_each_class() {
    let json = |path: &'static str, help: &'static str| {
        let mut parts = path.split_whitespace();
        let group = parts.next().unwrap();
        let leaf = parts.next().unwrap();
        Command::new(group).subcommand(
            Command::new(leaf).about(path.to_owned()).arg(
                Arg::new("json")
                    .long("json")
                    .action(ArgAction::SetTrue)
                    .help(help),
            ),
        )
    };
    let different_families = Command::new("fixture")
        .subcommand(json("account show", "account"))
        .subcommand(json("runner status", "runner"));
    assert!(
        !violations(&different_families)
            .iter()
            .any(|id| id.starts_with("option-help"))
    );
    assert_ne!(
        semantic_class("auth login", "json"),
        semantic_class("auth status", "json")
    );
    assert_eq!(
        semantic_class("auth login", "json"),
        semantic_class("auth identity link", "json")
    );
    assert_ne!(
        semantic_class("organization audit list", "limit"),
        semantic_class("organization list", "limit")
    );
    let limit = |help| Arg::new("limit").long("limit").help(help);
    let bounds = Command::new("fixture").subcommand(
        Command::new("organization")
            .subcommand(
                Command::new("audit")
                    .subcommand(Command::new("list").about("Audit").arg(limit("100"))),
            )
            .subcommand(
                Command::new("list")
                    .about("Organizations")
                    .arg(limit("200")),
            )
            .subcommand(
                Command::new("member")
                    .subcommand(Command::new("list").about("Members").arg(limit("changed"))),
            ),
    );
    let found = violations(&bounds);
    assert!(
        !found
            .iter()
            .any(|id| id.starts_with("option-help|limit:100"))
    );
    assert!(
        found
            .iter()
            .any(|id| id.starts_with("option-help|limit:200"))
    );
    let streaming = Command::new("fixture").subcommand(
        Command::new("auth")
            .subcommand(
                Command::new("login").about("Login").arg(
                    Arg::new("json")
                        .long("json")
                        .action(ArgAction::SetTrue)
                        .help("events"),
                ),
            )
            .subcommand(
                Command::new("identity").subcommand(
                    Command::new("link").about("Link").arg(
                        Arg::new("json")
                            .long("json")
                            .action(ArgAction::SetTrue)
                            .help("drift"),
                    ),
                ),
            ),
    );
    assert!(
        violations(&streaming)
            .iter()
            .any(|id| id.starts_with("option-help|json:streaming-events"))
    );
}

#[test]
fn baseline_rejects_unexpected_stale_and_duplicate_ids() {
    let observed = BTreeSet::from(["confirmation|entity remove".to_owned()]);
    assert!(
        check_baseline(&observed, &[])
            .unwrap_err()
            .contains("confirmation|entity remove")
    );
    assert!(
        check_baseline(&observed, &[("confirmation|other remove", "LIV-2427")])
            .unwrap_err()
            .contains("stale")
    );
    assert_eq!(
        check_baseline(
            &observed,
            &[
                ("confirmation|entity remove", "LIV-2427"),
                ("confirmation|entity remove", "LIV-2427")
            ]
        )
        .unwrap_err(),
        "unordered-or-duplicate|confirmation|entity remove"
    );
}
