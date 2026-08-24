use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;

const THREAD_ID: &str = "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12";
const CODEX_HOME: &str = "/synthetic/codex-home";
const SQLITE_HOME: &str = "/synthetic/sqlite-home";

fn parser(
    value_kind: AgentValueKind,
    maximum_response_bytes: u64,
    synthetic_model_provider: Option<&str>,
) -> CodexAppServerV1Parser {
    parser_with_system_prompt(
        value_kind,
        maximum_response_bytes,
        synthetic_model_provider,
        "scherzo system",
    )
}

fn parser_with_system_prompt(
    value_kind: AgentValueKind,
    maximum_response_bytes: u64,
    synthetic_model_provider: Option<&str>,
    system_prompt: &str,
) -> CodexAppServerV1Parser {
    CodexAppServerV1Parser::profile(
        Arc::from("/synthetic/project"),
        Arc::from(CODEX_HOME),
        Arc::from(SQLITE_HOME),
        Arc::from("0.147.0"),
        Arc::from("scherzo-loopback"),
        Arc::from("high"),
        Arc::from(system_prompt),
        vec![json!({"type": "text", "text": "user request"})],
        synthetic_model_provider.map(Arc::from),
        value_kind,
        NonZeroU64::new(maximum_response_bytes).unwrap(),
        CodexAppServerV1ProtocolLimits::profile(),
    )
    .unwrap()
}

fn take_json(parser: &mut CodexAppServerV1Parser) -> Value {
    let frame = parser.take_outbound().unwrap();
    assert_eq!(frame.last(), Some(&b'\n'));
    serde_json::from_slice(&frame[..frame.len() - 1]).unwrap()
}

fn result_envelope(result: Value) -> String {
    json!({"result": serde_json::to_string(&result).unwrap()}).to_string()
}

fn feed(
    parser: &mut CodexAppServerV1Parser,
    mut value: Value,
) -> Result<(ParserProgress, Vec<AgentObservation>), AgentFailureCause> {
    replace_fixture_thread_id(&mut value);
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    let mut observations = Vec::new();
    let progress = parser.push_stdout(&bytes, |observation| observations.push(observation))?;
    Ok((progress, observations))
}

fn replace_fixture_thread_id(value: &mut Value) {
    match value {
        Value::String(value) if value == "thread-1" => *value = THREAD_ID.to_owned(),
        Value::Array(values) => values.iter_mut().for_each(replace_fixture_thread_id),
        Value::Object(values) => values.values_mut().for_each(replace_fixture_thread_id),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn initialize(parser: &mut CodexAppServerV1Parser) {
    assert_eq!(
        take_json(parser),
        json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "scherzo-cloud",
                    "version": CLIENT_VERSION,
                }
            }
        })
    );
    feed(
        parser,
        json!({
            "id": 1,
            "result": {"userAgent": "codex/0.147.0", "codexHome": CODEX_HOME}
        }),
    )
    .unwrap();
    assert_eq!(
        take_json(parser),
        json!({"method": "initialized", "params": {}})
    );
    assert_eq!(
        take_json(parser),
        json!({
            "id": 2,
            "method": "config/read",
            "params": {"cwd": "/synthetic/project", "includeLayers": true}
        })
    );
}

fn effective_config(parser: &mut CodexAppServerV1Parser, provider: &str) -> Value {
    feed(
        parser,
        json!({
            "id": 2,
            "result": {
                "config": {
                    "developer_instructions": "native developer",
                    "sqlite_home": SQLITE_HOME,
                    "model_provider": provider,
                    "model_providers": {provider: {"wire_api": "responses"}},
                    "projects": {
                        "/synthetic/project": {"trust_level": "trusted"}
                    },
                    "hooks": {"enabled": true},
                    "mcp_servers": {"native": {"required": true}},
                },
                "origins": {"developer_instructions": {"name": {"type": "user"}}},
                "layers": [{"name": {"type": "user"}}],
            }
        }),
    )
    .unwrap();
    take_json(parser)
}

fn thread_start_response(provider: &str) -> Value {
    json!({
        "id": 3,
        "result": {
            "thread": {
                "id": "thread-1",
                "sessionId": "thread-1",
                "ephemeral": true,
                "path": null,
                "cliVersion": "0.147.0",
                "turns": [],
                "cwd": "/synthetic/project",
                "modelProvider": provider,
            },
            "model": "scherzo-loopback",
            "modelProvider": provider,
            "cwd": "/synthetic/project",
            "approvalPolicy": "never",
            "sandbox": {"type": "dangerFullAccess"},
        }
    })
}

fn thread_started_notification() -> Value {
    json!({
        "method": "thread/started",
        "params": {"thread": {
            "id": "thread-1",
            "sessionId": "thread-1",
            "ephemeral": true,
            "path": null,
            "cliVersion": "0.147.0",
            "cwd": "/synthetic/project",
        }}
    })
}

fn thread_response(parser: &mut CodexAppServerV1Parser, provider: &str) {
    feed(parser, thread_start_response(provider)).unwrap();
    let turn = take_json(parser);
    assert_eq!(turn["id"], 4);
    assert_eq!(turn["method"], "turn/start");
    assert_eq!(turn["params"]["threadId"], THREAD_ID);
    assert_eq!(turn["params"]["approvalPolicy"], "never");
    assert_eq!(
        turn["params"]["sandboxPolicy"],
        json!({"type": "externalSandbox", "networkAccess": "enabled"})
    );
    assert_eq!(turn["params"]["model"], "scherzo-loopback");
    assert_eq!(turn["params"]["effort"], "high");
    if parser.value_kind == AgentValueKind::Result {
        assert_eq!(turn["params"]["outputSchema"], weak_result_schema());
    } else {
        assert!(turn["params"].get("outputSchema").is_none());
    }
}

fn turn_response(parser: &mut CodexAppServerV1Parser) {
    feed(parser, thread_started_notification()).unwrap();
    feed(
        parser,
        json!({
            "id": 4,
            "result": {"turn": {"id": "turn-1", "items": [], "status": "inProgress"}}
        }),
    )
    .unwrap();
}

fn start(parser: &mut CodexAppServerV1Parser) -> Vec<AgentObservation> {
    let (progress, observations) = feed(
        parser,
        json!({
            "method": "turn/started",
            "params": {
                "threadId": "thread-1",
                "turn": {"id": "turn-1", "items": [], "status": "inProgress"},
            }
        }),
    )
    .unwrap();
    assert!(progress.start_acknowledged);
    observations
}

fn running_parser(
    value_kind: AgentValueKind,
    maximum_response_bytes: u64,
) -> CodexAppServerV1Parser {
    let mut parser = parser(value_kind, maximum_response_bytes, None);
    initialize(&mut parser);
    let thread = effective_config(&mut parser, "native-provider");
    assert!(thread["params"].get("modelProvider").is_none());
    assert_eq!(
        thread["params"]["developerInstructions"],
        "native developer\n\nscherzo system"
    );
    assert_eq!(thread["params"]["ephemeral"], true);
    assert_eq!(
        thread["params"]["config"],
        json!({"bypass_hook_trust": true})
    );
    thread_response(&mut parser, "native-provider");
    turn_response(&mut parser);
    start(&mut parser);
    parser
}

fn item_started(id: &str, kind: &str) -> Value {
    json!({
        "method": "item/started",
        "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {"id": id, "type": kind, "text": ""},
        }
    })
}

fn item_completed(id: &str, text: &str, phase: Value) -> Value {
    json!({
        "method": "item/completed",
        "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {"id": id, "type": "agentMessage", "text": text, "phase": phase},
        }
    })
}

fn turn_completed(items: Vec<Value>, status: &str) -> Value {
    json!({
        "method": "turn/completed",
        "params": {
            "threadId": "thread-1",
            "turn": {"id": "turn-1", "items": items, "status": status},
        }
    })
}

#[test]
fn setup_requests_preserve_native_resources_and_only_fixtures_select_a_provider() {
    let mut production = parser(AgentValueKind::None, 1024, None);
    initialize(&mut production);
    let thread = effective_config(&mut production, "host-provider");
    assert!(thread["params"].get("modelProvider").is_none());
    assert!(thread["params"].get("baseInstructions").is_none());
    assert_eq!(
        thread["params"]["developerInstructions"],
        "native developer\n\nscherzo system"
    );

    let mut synthetic = parser(AgentValueKind::None, 1024, Some("loopback"));
    initialize(&mut synthetic);
    let thread = effective_config(&mut synthetic, "loopback");
    assert_eq!(thread["params"]["modelProvider"], "loopback");
}

#[test]
fn admitted_text_attachment_fits_the_initial_native_turn() {
    let attachment = "x".repeat(usize::try_from(MAXIMUM_FRAME_BYTES).unwrap());
    let mut parser = CodexAppServerV1Parser::profile(
        Arc::from("/synthetic/project"),
        Arc::from(CODEX_HOME),
        Arc::from(SQLITE_HOME),
        Arc::from("0.147.0"),
        Arc::from("scherzo-loopback"),
        Arc::from("high"),
        Arc::from("scherzo system"),
        vec![
            json!({"type": "text", "text": "user request"}),
            json!({
                "type": "text",
                "text": format!(
                    "Scherzo attachment 000000 (text/plain) follows:\n{attachment}"
                ),
            }),
        ],
        None,
        AgentValueKind::None,
        NonZeroU64::new(1024).unwrap(),
        CodexAppServerV1ProtocolLimits::profile(),
    )
    .unwrap();
    initialize(&mut parser);
    let _ = effective_config(&mut parser, "native-provider");

    feed(
        &mut parser,
        json!({
            "id": 3,
            "result": {
                "thread": {
                    "id": "thread-1",
                    "sessionId": "thread-1",
                    "ephemeral": true,
                    "path": null,
                    "cliVersion": "0.147.0",
                    "turns": [],
                    "cwd": "/synthetic/project",
                    "modelProvider": "native-provider",
                },
                "model": "scherzo-loopback",
                "modelProvider": "native-provider",
                "cwd": "/synthetic/project",
                "approvalPolicy": "never",
                "sandbox": {"type": "dangerFullAccess"},
            }
        }),
    )
    .expect("an attachment below the admitted 256-MiB limit must reach Codex");
}

#[test]
fn empty_or_oversized_effective_instructions_fail_during_configuration() {
    let mut empty = parser_with_system_prompt(AgentValueKind::None, 1024, None, "");
    initialize(&mut empty);
    assert_eq!(
        feed(
            &mut empty,
            json!({
                "id": 2,
                "result": {
                    "config": {
                        "developer_instructions": "",
                        "sqlite_home": SQLITE_HOME,
                        "model_provider": "native-provider",
                    },
                    "origins": {},
                },
            }),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::EffectiveConfiguration,
        }
    );

    let mut oversized = parser_with_system_prompt(AgentValueKind::None, 1024, None, "12345");
    initialize(&mut oversized);
    oversized.limits.maximum_frame_bytes = NonZeroU64::new(4).unwrap();
    let response = json!({
        "id": 2,
        "result": {
            "config": {
                "developer_instructions": "",
                "sqlite_home": SQLITE_HOME,
                "model_provider": "native-provider",
            },
            "origins": {},
        },
    });
    assert_eq!(
        oversized
            .parse_response(response.as_object().unwrap())
            .unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::EffectiveConfiguration,
        }
    );
}

#[test]
fn matching_thread_notification_and_response_may_arrive_in_either_order() {
    let mut response_first = parser(AgentValueKind::None, 1024, None);
    initialize(&mut response_first);
    effective_config(&mut response_first, "native-provider");
    let (progress, observations) = feed(
        &mut response_first,
        thread_start_response("native-provider"),
    )
    .unwrap();
    assert!(!progress.start_acknowledged);
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::SessionEstablished,
        }
    )));
    assert_eq!(take_json(&mut response_first)["method"], "turn/start");
    let (progress, observations) =
        feed(&mut response_first, thread_started_notification()).unwrap();
    assert!(!progress.start_acknowledged);
    assert!(observations.is_empty());

    let mut notification_first = parser(AgentValueKind::None, 1024, None);
    initialize(&mut notification_first);
    effective_config(&mut notification_first, "native-provider");
    let (progress, observations) =
        feed(&mut notification_first, thread_started_notification()).unwrap();
    assert!(!progress.start_acknowledged);
    assert!(observations.is_empty());
    let (progress, observations) = feed(
        &mut notification_first,
        thread_start_response("native-provider"),
    )
    .unwrap();
    assert!(!progress.start_acknowledged);
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::SessionEstablished,
        }
    )));
    assert_eq!(take_json(&mut notification_first)["method"], "turn/start");

    let mut mismatched = parser(AgentValueKind::None, 1024, None);
    initialize(&mut mismatched);
    effective_config(&mut mismatched, "native-provider");
    feed(&mut mismatched, thread_started_notification()).unwrap();
    let mut response = thread_start_response("native-provider");
    response["result"]["thread"]["id"] =
        Value::String("018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e13".to_owned());
    assert_eq!(
        feed(&mut mismatched, response).unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ThreadStart,
        }
    );
}

#[test]
fn thread_scoped_warning_may_precede_thread_response() {
    let mut matching = parser(AgentValueKind::None, 1024, None);
    initialize(&mut matching);
    effective_config(&mut matching, "native-provider");
    let (progress, observations) = feed(
        &mut matching,
        json!({"method": "warning", "params": {
            "threadId": "thread-1",
            "message": "native startup warning",
        }}),
    )
    .unwrap();
    assert!(!progress.start_acknowledged);
    assert!(matches!(
        observations.as_slice(),
        [AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message,
        }] if message.as_ref() == "native startup warning"
    ));
    feed(&mut matching, thread_start_response("native-provider")).unwrap();
    assert_eq!(take_json(&mut matching)["method"], "turn/start");

    let mut mismatched = parser(AgentValueKind::None, 1024, None);
    initialize(&mut mismatched);
    effective_config(&mut mismatched, "native-provider");
    feed(
        &mut mismatched,
        json!({"method": "warning", "params": {
            "threadId": "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e13",
            "message": "native startup warning",
        }}),
    )
    .unwrap();
    assert_eq!(
        feed(&mut mismatched, thread_start_response("native-provider"),).unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ThreadStart,
        }
    );
}

#[test]
fn matching_turn_notification_acknowledges_start_after_the_response_in_either_order() {
    let mut response_first = parser(AgentValueKind::None, 1024, None);
    initialize(&mut response_first);
    effective_config(&mut response_first, "native-provider");
    thread_response(&mut response_first, "native-provider");
    turn_response(&mut response_first);
    let observations = start(&mut response_first);
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessStarted,
        }
    )));

    let mut notification_first = parser(AgentValueKind::None, 1024, None);
    initialize(&mut notification_first);
    effective_config(&mut notification_first, "native-provider");
    thread_response(&mut notification_first, "native-provider");
    let (progress, observations) = feed(
        &mut notification_first,
        json!({
            "method": "turn/started",
            "params": {"threadId": "thread-1", "turn": {
                "id": "turn-1", "items": [], "status": "inProgress"
            }}
        }),
    )
    .unwrap();
    assert!(!progress.start_acknowledged);
    assert!(observations.is_empty());
    let (progress, observations) = feed(
        &mut notification_first,
        json!({
            "id": 4,
            "result": {"turn": {"id": "turn-1", "items": [], "status": "inProgress"}}
        }),
    )
    .unwrap();
    assert!(progress.start_acknowledged);
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessStarted,
        }
    )));

    let mut mismatched = parser(AgentValueKind::None, 1024, None);
    initialize(&mut mismatched);
    effective_config(&mut mismatched, "native-provider");
    thread_response(&mut mismatched, "native-provider");
    feed(
        &mut mismatched,
        json!({
            "method": "turn/started",
            "params": {"threadId": "thread-1", "turn": {
                "id": "turn-1", "items": [], "status": "inProgress"
            }}
        }),
    )
    .unwrap();
    assert_eq!(
        feed(
            &mut mismatched,
            json!({
                "id": 4,
                "result": {"turn": {"id": "other-turn", "items": [], "status": "inProgress"}}
            }),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::TurnStart,
        }
    );

    let mut batched = parser(AgentValueKind::None, 1024, None);
    initialize(&mut batched);
    effective_config(&mut batched, "native-provider");
    thread_response(&mut batched, "native-provider");
    turn_response(&mut batched);
    let frames = [
        json!({
            "method": "turn/started",
            "params": {
                "threadId": THREAD_ID,
                "turn": {"id": "turn-1", "items": [], "status": "inProgress"},
            }
        }),
        json!({
            "method": "item/started",
            "params": {
                "threadId": "other-thread",
                "turnId": "turn-1",
                "item": {"id": "item-1", "type": "agentMessage", "text": ""},
            }
        }),
    ];
    let mut bytes = Vec::new();
    for frame in frames {
        serde_json::to_writer(&mut bytes, &frame).unwrap();
        bytes.push(b'\n');
    }
    let mut observations = Vec::new();
    assert_eq!(
        batched
            .push_stdout(&bytes, |observation| observations.push(observation))
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert!(batched.start_acknowledged());
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::HarnessStarted,
        }
    )));
}

#[test]
fn stale_duplicate_cross_thread_cross_turn_and_cross_item_identities_fail_closed() {
    let mut stale = parser(AgentValueKind::None, 1024, None);
    initialize(&mut stale);
    assert_eq!(
        feed(&mut stale, json!({"id": 1, "result": {}})).unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::EffectiveConfiguration,
        }
    );

    let mut cross_thread = running_parser(AgentValueKind::None, 1024);
    assert_eq!(
        feed(
            &mut cross_thread,
            json!({
                "method": "item/started",
                "params": {"threadId": "other", "turnId": "turn-1", "item": {
                    "id": "item-1", "type": "agentMessage", "text": ""
                }}
            })
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );

    let mut cross_turn = running_parser(AgentValueKind::None, 1024);
    assert_eq!(
        feed(
            &mut cross_turn,
            json!({
                "method": "item/started",
                "params": {"threadId": "thread-1", "turnId": "other", "item": {
                    "id": "item-1", "type": "agentMessage", "text": ""
                }}
            })
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );

    let mut cross_item = running_parser(AgentValueKind::None, 1024);
    feed(&mut cross_item, item_started("item-1", "agentMessage")).unwrap();
    assert_eq!(
        feed(
            &mut cross_item,
            json!({
                "method": "item/agentMessage/delta",
                "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": "other", "delta": "x"}
            })
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );

    let mut duplicate = running_parser(AgentValueKind::None, 1024);
    feed(&mut duplicate, item_started("item-1", "agentMessage")).unwrap();
    assert_eq!(
        feed(&mut duplicate, item_started("item-1", "agentMessage")).unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );

    let mut duplicate_response = running_parser(AgentValueKind::None, 1024);
    assert_eq!(
        feed(
            &mut duplicate_response,
            json!({"id": 4, "result": {"turn": {"id": "turn-1", "items": [], "status": "inProgress"}}})
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
}

#[test]
fn completed_final_answer_is_authoritative_and_deltas_are_observations_only() {
    let mut parser = running_parser(AgentValueKind::Response, 5);
    feed(&mut parser, item_started("commentary", "agentMessage")).unwrap();
    feed(
        &mut parser,
        json!({
            "method": "item/agentMessage/delta",
            "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": "commentary", "delta": "provisional"}
        }),
    )
    .unwrap();
    feed(
        &mut parser,
        item_completed("commentary", "ignore", json!("commentary")),
    )
    .unwrap();
    feed(&mut parser, item_started("final", "agentMessage")).unwrap();
    feed(
        &mut parser,
        item_completed("final", "12345", json!("final_answer")),
    )
    .unwrap();
    let (progress, _) = feed(
        &mut parser,
        turn_completed(
            vec![
                json!({"id": "commentary", "type": "agentMessage", "text": "ignore", "phase": "commentary"}),
                json!({"id": "final", "type": "agentMessage", "text": "12345", "phase": "final_answer"}),
            ],
            "completed",
        ),
    )
    .unwrap();
    assert!(progress.close_standard_input);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = parser.finish(true)
    else {
        panic!("exact-limit completed final answer must be authoritative");
    };
    assert_eq!(response.as_str(), "12345");
}

#[test]
fn completed_turn_with_native_error_fails_closed_without_committing_response() {
    let mut parser = running_parser(AgentValueKind::Response, 1024);
    feed(&mut parser, item_started("final", "agentMessage")).unwrap();
    feed(
        &mut parser,
        item_completed("final", "response", json!("final_answer")),
    )
    .unwrap();

    assert_eq!(
        feed(
            &mut parser,
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": "turn-1",
                        "items": [{
                            "id": "final",
                            "type": "agentMessage",
                            "text": "response",
                            "phase": "final_answer",
                        }],
                        "status": "completed",
                        "error": {"message": "native terminal failure"},
                    },
                },
            }),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_eq!(
        parser.finish(true),
        AgentOutcome::Failed(AgentFailureCause::HarnessProtocolFailed.into())
    );
}

#[test]
fn unsupported_completed_tool_status_fails_closed() {
    for (kind, status) in [
        ("commandExecution", "unsupported"),
        ("fileChange", "unsupported"),
        ("mcpToolCall", "unsupported"),
        ("mcpToolCall", "declined"),
    ] {
        let mut parser = running_parser(AgentValueKind::None, 1024);
        let tool = |status| {
            json!({
                "id": "tool",
                "type": kind,
                "status": status,
                "aggregatedOutput": "",
            })
        };
        feed(
            &mut parser,
            json!({
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": tool("inProgress"),
                },
            }),
        )
        .unwrap();

        assert_eq!(
            feed(
                &mut parser,
                json!({
                    "method": "item/completed",
                    "params": {
                        "threadId": "thread-1",
                        "turnId": "turn-1",
                        "item": tool(status),
                    },
                }),
            )
            .unwrap_err(),
            AgentFailureCause::HarnessProtocolFailed,
            "{kind}:{status}",
        );
    }
}

#[test]
fn aggregate_retained_agent_message_text_is_bounded() {
    let mut parser = running_parser(AgentValueKind::Response, 1024);
    parser.limits.maximum_retained_agent_message_bytes = NonZeroU64::new(5).unwrap();
    feed(&mut parser, item_started("first", "agentMessage")).unwrap();
    feed(
        &mut parser,
        item_completed("first", "123", json!("final_answer")),
    )
    .unwrap();
    feed(&mut parser, item_started("second", "agentMessage")).unwrap();

    assert_eq!(
        feed(
            &mut parser,
            item_completed("second", "456", json!("commentary")),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_eq!(
        parser.finish(true),
        AgentOutcome::Failed(AgentFailureCause::HarnessProtocolFailed.into())
    );
}

#[test]
fn absent_empty_oversized_delta_only_and_failed_outputs_never_commit() {
    for messages in [
        vec![],
        vec![json!({
            "id": "empty", "type": "agentMessage", "text": "", "phase": "final_answer"
        })],
    ] {
        let mut parser = running_parser(AgentValueKind::Response, 5);
        if !messages.is_empty() {
            feed(&mut parser, item_started("empty", "agentMessage")).unwrap();
            feed(
                &mut parser,
                item_completed("empty", "", json!("final_answer")),
            )
            .unwrap();
        }
        feed(&mut parser, turn_completed(messages, "completed")).unwrap();
        assert_eq!(
            parser.finish(true),
            AgentOutcome::Completed(CompletedAgentInvocation::NoResponse)
        );
    }

    let mut oversized = running_parser(AgentValueKind::Response, 5);
    feed(&mut oversized, item_started("large", "agentMessage")).unwrap();
    assert_eq!(
        feed(
            &mut oversized,
            item_completed("large", "123456", json!("final_answer"))
        )
        .unwrap_err(),
        AgentFailureCause::CapturedValueTooLarge
    );

    let mut delta_only = running_parser(AgentValueKind::Response, 5);
    feed(&mut delta_only, item_started("delta", "agentMessage")).unwrap();
    feed(
        &mut delta_only,
        json!({
            "method": "item/agentMessage/delta",
            "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": "delta", "delta": "12345"}
        }),
    )
    .unwrap();
    assert_eq!(
        feed(&mut delta_only, turn_completed(vec![], "completed")).unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );

    for status in ["failed", "interrupted"] {
        let mut parser = running_parser(AgentValueKind::Response, 5);
        feed(&mut parser, item_started("final", "agentMessage")).unwrap();
        feed(
            &mut parser,
            item_completed("final", "12345", json!("final_answer")),
        )
        .unwrap();
        feed(
            &mut parser,
            turn_completed(
                vec![json!({"id": "final", "type": "agentMessage", "text": "12345", "phase": "final_answer"})],
                status,
            ),
        )
        .unwrap();
        assert!(matches!(
            parser.finish(true),
            AgentOutcome::Failed(failure)
                if matches!(failure.cause(), AgentFailureCause::HarnessFailed { .. })
        ));
    }
}

#[test]
fn malformed_wrapped_duplicate_and_missing_result_candidates_never_commit() {
    for text in [
        "not-json",
        "```json\n{\"result\":\"1\"}\n```",
        "{\"result\":\"1\",\"result\":\"2\"}",
        "{\"result\":\"1\",\"extra\":true}",
        "{\"result\":\"not-json\"}",
        "{\"result\":\"{\\\"decision\\\":\\\"recheck\\\",\\\"decision\\\":\\\"gave_up\\\"}\"}",
    ] {
        let mut parser = running_parser(AgentValueKind::Result, 1024);
        feed(&mut parser, item_started("result", "agentMessage")).unwrap();
        feed(
            &mut parser,
            item_completed("result", text, json!("final_answer")),
        )
        .unwrap();
        assert_eq!(
            feed(
                &mut parser,
                turn_completed(
                    vec![json!({
                        "id": "result",
                        "type": "agentMessage",
                        "text": text,
                        "phase": "final_answer",
                    })],
                    "completed",
                ),
            )
            .unwrap_err(),
            AgentFailureCause::HarnessProtocolFailed,
            "{text}",
        );
        assert!(matches!(parser.finish(true), AgentOutcome::Failed(_)));
    }

    let mut duplicate = running_parser(AgentValueKind::Result, 1024);
    for id in ["first", "second"] {
        feed(&mut duplicate, item_started(id, "agentMessage")).unwrap();
        feed(
            &mut duplicate,
            item_completed(id, "{\"result\":\"1\"}", json!("final_answer")),
        )
        .unwrap();
    }
    assert_eq!(
        feed(
            &mut duplicate,
            turn_completed(
                vec![
                    json!({"id": "first", "type": "agentMessage", "text": "{\"result\":\"1\"}", "phase": "final_answer"}),
                    json!({"id": "second", "type": "agentMessage", "text": "{\"result\":\"1\"}", "phase": "final_answer"}),
                ],
                "completed",
            ),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut missing = running_parser(AgentValueKind::Result, 1024);
    let (progress, _) = feed(&mut missing, turn_completed(vec![], "completed")).unwrap();
    assert!(progress.close_standard_input);
    assert_eq!(
        missing.finish(true),
        AgentOutcome::Failed(AgentFailureCause::MissingResult.into()),
    );
}

#[test]
fn one_rejection_queues_one_same_thread_correction_then_exhausts() {
    let mut parser = running_parser(AgentValueKind::Result, 1024);
    for (id, value, turn_id) in [("first", -1, "turn-1"), ("second", 0, "turn-2")] {
        let text = result_envelope(json!(value));
        feed(
            &mut parser,
            json!({
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": turn_id,
                    "item": {"id": id, "type": "agentMessage", "text": ""},
                },
            }),
        )
        .unwrap();
        feed(
            &mut parser,
            json!({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": turn_id,
                    "item": {"id": id, "type": "agentMessage", "text": text, "phase": "final_answer"},
                },
            }),
        )
        .unwrap();
        feed(
            &mut parser,
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {
                        "id": turn_id,
                        "items": [{
                            "id": id,
                            "type": "agentMessage",
                            "text": text,
                            "phase": "final_answer",
                        }],
                        "status": "completed",
                    },
                },
            }),
        )
        .unwrap();
        assert_eq!(
            parser.take_result_candidate().unwrap().as_ref(),
            &json!(value)
        );
        let progress = parser
            .reject_result(Arc::from("bounded authoritative feedback"))
            .unwrap();
        if id == "first" {
            assert!(!progress.close_standard_input);
            let correction = take_json(&mut parser);
            assert_eq!(correction["id"], 6);
            assert_eq!(correction["method"], "turn/start");
            assert_eq!(correction["params"]["threadId"], THREAD_ID);
            assert_eq!(
                correction["params"]["input"],
                json!([{"type": "text", "text": "bounded authoritative feedback"}]),
            );
            assert_eq!(correction["params"]["outputSchema"], weak_result_schema());
            feed(
                &mut parser,
                json!({"id": 6, "result": {"turn": {"id": "turn-2", "items": [], "status": "inProgress"}}}),
            )
            .unwrap();
            let (started, _) = feed(
                &mut parser,
                json!({
                    "method": "turn/started",
                    "params": {
                        "threadId": "thread-1",
                        "turn": {"id": "turn-2", "items": [], "status": "inProgress"},
                    },
                }),
            )
            .unwrap();
            assert!(!started.start_acknowledged);
        } else {
            assert!(progress.close_standard_input);
            assert_eq!(
                parser.finish(true),
                AgentOutcome::Failed(AgentFailureCause::MissingResult.into()),
            );
            break;
        }
    }
}

#[test]
fn malformed_oversized_truncated_and_correlation_inputs_are_bounded_by_phase() {
    let mut malformed = parser(AgentValueKind::None, 1024, None);
    assert_eq!(
        malformed.push_stdout(b"not-json\n", |_| {}).unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::Initialization,
        }
    );

    let mut invalid_utf8 = parser(AgentValueKind::None, 1024, None);
    assert_eq!(
        invalid_utf8
            .push_stdout(&[0xff, b'\n'], |_| {})
            .unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::Initialization,
        }
    );

    let limits = CodexAppServerV1ProtocolLimits::with_limits(
        NonZeroU64::new(8).unwrap(),
        NonZeroU64::new(64).unwrap(),
    );
    let mut oversized = parser(AgentValueKind::None, 8, None);
    oversized.limits = limits;
    assert_eq!(
        oversized.push_stdout(b"123456789", |_| {}).unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::Initialization,
        }
    );

    let mut truncated = parser(AgentValueKind::None, 1024, None);
    truncated.push_stdout(b"{\"id\":1", |_| {}).unwrap();
    assert_eq!(
        truncated.finish(true),
        AgentOutcome::Failed(
            AgentFailureCause::HarnessSetupFailed {
                stage: AgentHarnessSetupStage::Initialization,
            }
            .into(),
        )
    );

    let mut correlation_limited = parser(AgentValueKind::None, 1024, None);
    correlation_limited.limits = CodexAppServerV1ProtocolLimits::with_limits(
        CodexAppServerV1ProtocolLimits::profile().maximum_frame_bytes(),
        NonZeroU64::new(7).unwrap(),
    );
    initialize(&mut correlation_limited);
    effective_config(&mut correlation_limited, "native-provider");
    assert_eq!(
        feed(
            &mut correlation_limited,
            json!({
                "id": 3,
                "result": {
                    "thread": {
                        "id": "thread-1",
                        "sessionId": "thread-1",
                        "ephemeral": true,
                        "path": null,
                        "cliVersion": "0.147.0",
                        "turns": [],
                        "cwd": "/synthetic/project",
                        "modelProvider": "native-provider",
                    },
                    "model": "scherzo-loopback",
                    "modelProvider": "native-provider",
                    "cwd": "/synthetic/project",
                    "approvalPolicy": "never",
                    "sandbox": {"type": "dangerFullAccess"},
                }
            })
        )
        .unwrap_err(),
        AgentFailureCause::HarnessSetupFailed {
            stage: AgentHarnessSetupStage::ThreadStart,
        }
    );

    let mut diagnostic_limited = running_parser(AgentValueKind::None, 1024);
    diagnostic_limited.limits.maximum_retained_diagnostic_bytes = NonZeroU64::new(5).unwrap();
    assert_eq!(
        feed(
            &mut diagnostic_limited,
            json!({"method": "warning", "params": {
                "threadId": "thread-1", "message": "123456"
            }}),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut unsafe_diagnostic = running_parser(AgentValueKind::None, 1024);
    let (_, observations) = feed(
        &mut unsafe_diagnostic,
        json!({"method": "warning", "params": {
            "threadId": "thread-1", "message": "unsafe\u{1b}diagnostic"
        }}),
    )
    .unwrap();
    assert!(matches!(
        observations.as_slice(),
        [AgentObservation::Diagnostic { message, .. }]
            if message.as_ref() == "unsafe\\u{1b}diagnostic"
    ));
}

#[test]
fn completed_terminal_before_start_acknowledgement_fails_closed() {
    let mut parser = parser(AgentValueKind::None, 1024, None);
    initialize(&mut parser);
    effective_config(&mut parser, "native-provider");
    thread_response(&mut parser, "native-provider");
    turn_response(&mut parser);

    let _ = feed(&mut parser, turn_completed(vec![], "completed"));
    let outcome = parser.finish(true);
    assert!(
        matches!(outcome, AgentOutcome::Failed(_)),
        "completion before the authoritative turn/started boundary must fail closed: {outcome:?}",
    );
}

#[test]
fn nonretry_native_error_cannot_be_erased_by_completed_terminal() {
    let mut parser = running_parser(AgentValueKind::Response, 1024);
    feed(&mut parser, item_started("final", "agentMessage")).unwrap();
    let final_item = json!({
        "id": "final",
        "type": "agentMessage",
        "text": "must not commit",
        "phase": "final_answer",
    });
    feed(
        &mut parser,
        item_completed("final", "must not commit", json!("final_answer")),
    )
    .unwrap();
    feed(
        &mut parser,
        json!({"method": "error", "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "native failure", "codexErrorInfo": "unauthorized"},
            "willRetry": false,
        }}),
    )
    .unwrap();

    let _ = feed(&mut parser, turn_completed(vec![final_item], "completed"));
    let outcome = parser.finish(true);
    assert!(
        matches!(outcome, AgentOutcome::Failed(_)),
        "a non-retryable native failure must survive a contradictory completed terminal: {outcome:?}",
    );
}

#[test]
fn bounded_native_error_prose_does_not_replace_structured_identity() {
    let mut parser = running_parser(AgentValueKind::None, 1024);
    parser.limits.maximum_retained_diagnostic_bytes = NonZeroU64::new(5).unwrap();
    let native_error = feed(
        &mut parser,
        json!({"method": "error", "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "123456", "codexErrorInfo": "unauthorized"},
            "willRetry": false,
        }}),
    );
    assert!(
        native_error.is_ok(),
        "bounded diagnostic prose must not replace codexErrorInfo identity: {native_error:?}",
    );
    feed(
        &mut parser,
        json!({"method": "turn/completed", "params": {
            "threadId": "thread-1",
            "turn": {
                "id": "turn-1",
                "items": [],
                "status": "failed",
                "error": {"message": "x", "codexErrorInfo": "unauthorized"},
            },
        }}),
    )
    .unwrap();
    assert_eq!(
        parser.finish(true),
        AgentOutcome::Failed(
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            }
            .into(),
        ),
    );
}

#[test]
fn schema_invalid_known_server_request_fails_closed() {
    for (method, item_kind, params) in [
        (
            "item/commandExecution/requestApproval",
            Some("commandExecution"),
            json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "interactive",
            }),
        ),
        (
            "item/fileChange/requestApproval",
            Some("fileChange"),
            json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "interactive",
                "startedAtMs": "not-an-integer",
            }),
        ),
        (
            "item/permissions/requestApproval",
            Some("commandExecution"),
            json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "interactive",
                "startedAtMs": 1,
                "cwd": "/synthetic/project",
            }),
        ),
        (
            "item/tool/requestUserInput",
            Some("commandExecution"),
            json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "interactive",
                "isBlocking": true,
                "questions": [{}],
            }),
        ),
        (
            "mcpServer/elicitation/request",
            None,
            json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "serverName": "fixture-mcp",
                "mode": "form",
                "message": "fixture",
                "requestedSchema": {"type": "object"},
            }),
        ),
    ] {
        let mut parser = running_parser(AgentValueKind::None, 1024);
        if let Some(item_kind) = item_kind {
            feed(&mut parser, item_started("interactive", item_kind)).unwrap();
        }
        let request = feed(
            &mut parser,
            json!({
                "id": "invalid-request",
                "method": method,
                "params": params,
            }),
        );
        assert!(
            request.is_err(),
            "a schema-invalid {method} request must fail closed: {request:?}",
        );
    }
}

#[test]
fn active_turn_subtype_is_part_of_correlated_error_identity() {
    let mut parser = running_parser(AgentValueKind::None, 1024);
    feed(
        &mut parser,
        json!({"method": "error", "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "native", "codexErrorInfo": {
                "activeTurnNotSteerable": {"turnKind": "review"}
            }},
            "willRetry": false,
        }}),
    )
    .unwrap();
    assert_eq!(
        feed(
            &mut parser,
            json!({"method": "turn/completed", "params": {
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1",
                    "items": [],
                    "status": "failed",
                    "error": {"message": "terminal", "codexErrorInfo": {
                        "activeTurnNotSteerable": {"turnKind": "compact"}
                    }},
                },
            }}),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn retry_exhaustion_requires_a_retrying_native_observation() {
    let mut parser = running_parser(AgentValueKind::None, 1024);
    feed(
        &mut parser,
        json!({"method": "error", "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "native", "codexErrorInfo": "unauthorized"},
            "willRetry": false,
        }}),
    )
    .unwrap();
    assert_eq!(
        feed(
            &mut parser,
            json!({"method": "turn/completed", "params": {
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1",
                    "items": [],
                    "status": "failed",
                    "error": {"message": "terminal", "codexErrorInfo": {
                        "responseTooManyFailedAttempts": {}
                    }},
                },
            }}),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn pending_server_responses_are_aggregate_bounded() {
    let mut parser = running_parser(AgentValueKind::None, 1024);
    parser.limits.maximum_frame_bytes = NonZeroU64::new(1024).unwrap();
    feed(&mut parser, item_started("command", "commandExecution")).unwrap();
    let mut requests = Vec::new();
    for id in 0..40 {
        serde_json::to_writer(
            &mut requests,
            &json!({
                "id": id,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "itemId": "command",
                    "startedAtMs": 1,
                },
            }),
        )
        .unwrap();
        requests.push(b'\n');
    }
    assert_eq!(
        parser.push_stdout(&requests, |_| {}).unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn contradictory_native_and_terminal_error_identity_fails_closed() {
    let mut parser = running_parser(AgentValueKind::Response, 1024);
    feed(
        &mut parser,
        json!({"method": "error", "params": {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "diagnostic one", "codexErrorInfo": "unauthorized"},
            "willRetry": false,
        }}),
    )
    .unwrap();
    assert_eq!(
        feed(
            &mut parser,
            json!({"method": "turn/completed", "params": {
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1",
                    "items": [],
                    "status": "failed",
                    "error": {"message": "diagnostic two", "codexErrorInfo": "badRequest"},
                },
            }}),
        )
        .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed,
    );
    assert_eq!(
        parser.finish(true),
        AgentOutcome::Failed(AgentFailureCause::HarnessProtocolFailed.into()),
    );
}
