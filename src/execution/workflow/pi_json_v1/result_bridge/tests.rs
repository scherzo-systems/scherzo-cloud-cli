use std::fs;
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Arc;
use std::time::Duration;

use jsonschema::Validator;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;
use crate::execution::workflow::agent::WorkflowRunId;
use crate::execution::workflow::runtime::{ActionId, TransitionSequence};

const RETAINED_FIXTURE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/fixtures/workflow-result.schema.json"
));
const MATERIALIZATION_INPUT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/fixtures/materialization-input.json"
));
const GENERATED_EXTENSION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/execution/workflow/pi-json-v1-extension/fixtures/generated/pi-json-v1-extension.ts"
));

fn retained(bytes: &[u8]) -> RetainedJsonSchema {
    let document = Arc::new(serde_json::from_slice(bytes).unwrap());
    RetainedJsonSchema::compile(Arc::from(bytes), document).unwrap()
}

#[derive(Clone)]
enum TestClock {
    Pending,
    Controlled {
        registrations: tokio::sync::mpsc::UnboundedSender<Duration>,
        release: tokio::sync::watch::Receiver<bool>,
    },
}

impl CoordinatorClock for TestClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        let Self::Controlled {
            registrations,
            release,
        } = self
        else {
            return std::future::pending().await;
        };
        let _ = registrations.send(deadline);
        let mut release = release.clone();
        while !*release.borrow_and_update() {
            if release.changed().await.is_err() {
                return;
            }
        }
    }
}

fn receive_deadline() -> PositiveDuration {
    PositiveDuration::new(Duration::from_secs(5)).unwrap()
}

fn fixed_identity(run: &str, step: &str) -> AgentInvocationIdentity {
    AgentInvocationIdentity::new(
        WorkflowRunId::from(Arc::from(run)),
        Arc::from(step),
        ActionId {
            transition_sequence: TransitionSequence::default(),
        },
    )
}

fn wrapper_validator(wrapper: &Value) -> Validator {
    Validator::options()
        .with_draft(Draft::Draft202012)
        .with_pattern_options(PatternOptions::regex())
        .with_retriever(RejectRetrieval)
        .build(wrapper)
        .unwrap()
}

#[test]
fn resource_wrappers_preserve_fragment_roots_and_retained_bytes() {
    let cases = [
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {"Value": {"type": "string"}},
                "$ref": "#/$defs/Value"
            }),
            json!({"result": "value"}),
            json!({"result": 1}),
            "string",
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {
                    "Value": {
                        "type": "object",
                        "properties": {"detail": {"$ref": "#/$defs/Detail"}},
                        "required": ["detail"]
                    },
                    "Detail": {
                        "type": "object",
                        "properties": {"code": {"type": "string"}},
                        "required": ["code"]
                    }
                },
                "$ref": "#/$defs/Value"
            }),
            json!({"result": {"detail": {"code": "ok"}}}),
            json!({"result": {"detail": {"code": 1}}}),
            "object",
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {"Value": {"$anchor": "value", "type": "integer"}},
                "$ref": "#value"
            }),
            json!({"result": 1}),
            json!({"result": "value"}),
            "integer",
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$defs": {
                    "Value": {
                        "$dynamicAnchor": "value",
                        "type": "array",
                        "items": {"$dynamicRef": "#value"}
                    }
                },
                "$ref": "#value"
            }),
            json!({"result": []}),
            json!({"result": [1]}),
            "array",
        ),
        (
            json!({
                "$schema": JSON_SCHEMA_DIALECT,
                "$id": "https://author.example/result.json",
                "$defs": {"Value": {"type": "boolean"}},
                "$ref": "#/$defs/Value"
            }),
            json!({"result": true}),
            json!({"result": "true"}),
            "boolean",
        ),
    ];

    for (document, valid, invalid, expected_native_type) in cases {
        let retained_bytes = serde_json::to_vec_pretty(&document).unwrap();
        let schema = retained(&retained_bytes);
        let retained_document = schema.document().clone();
        let transport = derive_transport_schema(&schema).unwrap();
        let validator = wrapper_validator(&transport.complete_wrapper);
        let native_validator = wrapper_validator(&transport.native_parameters);

        assert!(validator.is_valid(&valid));
        assert!(!validator.is_valid(&invalid));
        assert!(native_validator.is_valid(&valid));
        assert!(!native_validator.is_valid(&invalid));
        assert_eq!(schema.bytes(), retained_bytes);
        assert_eq!(schema.document(), &retained_document);
        let expected_resource_id = document
            .get("$id")
            .cloned()
            .unwrap_or_else(|| Value::String(transport.resource_id.clone()));
        assert_eq!(
            transport.complete_wrapper["$defs"]["workflowResult"]["$id"],
            expected_resource_id
        );
        assert_eq!(
            transport.complete_wrapper["properties"]["result"]["$ref"],
            expected_resource_id
        );
        assert_eq!(
            Value::String(transport.resource_id.clone()),
            expected_resource_id
        );
        if document.get("$id").is_some() {
            assert_eq!(
                transport.complete_wrapper["$defs"]["workflowResult"],
                document
            );
        }
        let native_result = &transport.native_parameters["properties"]["result"];
        assert_eq!(native_result["type"], expected_native_type);
        assert!(native_result.get("$ref").is_none());
        assert!(
            !serde_json::to_string(&transport.native_parameters)
                .unwrap()
                .contains(RESOURCE_ID_PREFIX)
        );
    }
}

#[test]
fn repeated_reference_targets_are_expanded_once() {
    let mut definitions = serde_json::Map::new();
    for level in (0..=4).rev() {
        let schema = if level == 4 {
            json!({
                "type": "object",
                "properties": {"detail": {"type": "string"}},
                "required": ["detail"],
                "additionalProperties": false
            })
        } else {
            let next = format!("#/$defs/Level{}", level + 1);
            json!({"$ref": next, "$dynamicRef": next})
        };
        definitions.insert(format!("Level{level}"), schema);
    }
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "$defs": definitions,
        "$ref": "#/$defs/Level0"
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();
    let native_result = &transport.native_parameters["properties"]["result"];
    let candidate = json!({"result": {"detail": "ok"}});

    assert_eq!(native_result["type"], "object");
    assert_eq!(
        native_result["$dynamicRef"],
        "#/properties/result/$defs/Level4"
    );
    assert!(wrapper_validator(&transport.native_parameters).is_valid(&candidate));
}

#[test]
fn reference_expansion_budget_retains_a_valid_local_reference() {
    let mut definitions = serde_json::Map::new();
    for level in (0..=MAX_MODEL_REFERENCE_EXPANSIONS).rev() {
        let schema = if level == MAX_MODEL_REFERENCE_EXPANSIONS {
            json!({"type": "object"})
        } else {
            json!({"$ref": format!("#/$defs/Level{}", level + 1)})
        };
        definitions.insert(format!("Level{level}"), schema);
    }
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "$defs": definitions,
        "$ref": "#/$defs/Level0"
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();
    let native_result = &transport.native_parameters["properties"]["result"];
    let candidate = json!({"result": {}});

    assert_eq!(
        native_result["$ref"],
        format!(
            "#/properties/result/$defs/Level{}",
            MAX_MODEL_REFERENCE_EXPANSIONS
        )
    );
    assert!(wrapper_validator(&transport.native_parameters).is_valid(&candidate));
}

#[test]
fn nested_object_result_shape_is_exposed_inline() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "properties": {
            "summary": {"type": "string", "minLength": 1},
            "details": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {"label": {"type": "string"}},
                            "required": ["label"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["items"],
                "additionalProperties": false
            }
        },
        "required": ["summary", "details"],
        "additionalProperties": false
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();
    let mut expected_result = document;
    expected_result.as_object_mut().unwrap().remove("$schema");

    assert_eq!(
        transport.native_parameters["properties"]["result"],
        expected_result
    );
    assert!(transport.native_parameters.get("$defs").is_none());
    assert!(
        transport.native_parameters["properties"]["result"]
            .get("$ref")
            .is_none()
    );
}

#[test]
fn regex_constraints_project_to_nested_structural_guidance() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "properties": {
            "report": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "minLength": 3,
                        "pattern": "^[a-z]+$"
                    },
                    "sections": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "heading": {
                                    "type": "string",
                                    "pattern": "^[A-Z]"
                                }
                            },
                            "required": ["heading"],
                            "additionalProperties": false
                        }
                    }
                },
                "patternProperties": {
                    "^x-": {
                        "type": "object",
                        "properties": {
                            "values": {
                                "type": "array",
                                "items": {"type": "integer", "minimum": 0}
                            }
                        },
                        "required": ["values"],
                        "additionalProperties": false
                    }
                },
                "required": ["title", "sections"],
                "additionalProperties": false
            }
        },
        "required": ["report"],
        "additionalProperties": false
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();

    assert_eq!(
        transport.native_parameters["properties"]["result"],
        json!({
            "type": "object",
            "properties": {
                "report": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "minLength": 3},
                        "sections": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "properties": {"heading": {"type": "string"}},
                                "required": ["heading"],
                                "additionalProperties": false
                            }
                        }
                    },
                    "required": ["title", "sections"],
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "values": {
                                "type": "array",
                                "items": {"type": "integer", "minimum": 0}
                            }
                        },
                        "required": ["values"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["report"],
            "additionalProperties": false
        })
    );
    assert!(
        transport.complete_wrapper["$defs"]["workflowResult"]["properties"]["report"]
            .get("patternProperties")
            .is_some()
    );
}

#[test]
fn regex_projection_does_not_reject_values_allowed_by_negation() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "properties": {
            "code": {
                "type": "string",
                "not": {"pattern": "^bad$"}
            }
        },
        "required": ["code"],
        "additionalProperties": false
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();
    let candidate = json!({"code": "ok"});

    assert!(schema.is_valid(&candidate));
    assert!(
        wrapper_validator(&transport.native_parameters).is_valid(&json!({"result": candidate}))
    );
}

#[test]
fn compatibility_shape_does_not_replace_reference_and_regex_authority() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "$id": "https://author.example/result.json",
        "$defs": {
            "Outcome": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "pattern": "^ok-[0-9]+$"}
                },
                "required": ["code"],
                "additionalProperties": false
            }
        },
        "$ref": "#/$defs/Outcome"
    });
    let retained_bytes = serde_json::to_vec_pretty(&document).unwrap();
    let schema = retained(&retained_bytes);
    let transport = derive_transport_schema(&schema).unwrap();
    let validator = wrapper_validator(&transport.complete_wrapper);
    let conforming = json!({"result": {"code": "ok-12"}});
    let encoded_string = json!({"result": r#"{"code":"ok-12"}"#});
    let regex_invalid = json!({"result": {"code": "not-ok"}});

    assert_eq!(
        transport.native_parameters["properties"]["result"],
        json!({
            "$defs": {
                "Outcome": {
                    "type": "object",
                    "properties": {"code": {"type": "string"}},
                    "required": ["code"],
                    "additionalProperties": false
                }
            },
            "type": "object",
            "properties": {"code": {"type": "string"}},
            "required": ["code"],
            "additionalProperties": false
        })
    );
    assert!(validator.is_valid(&conforming));
    assert!(!validator.is_valid(&encoded_string));
    assert!(!validator.is_valid(&regex_invalid));
    assert!(schema.is_valid(&conforming["result"]));
    assert!(!schema.is_valid(&encoded_string["result"]));
    assert!(!schema.is_valid(&regex_invalid["result"]));
    assert_eq!(
        transport.complete_wrapper["$defs"]["workflowResult"],
        document
    );
    assert_eq!(schema.bytes(), retained_bytes);
}

#[test]
fn inlined_reference_preserves_root_unevaluated_properties_scope() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "description": "Root result",
        "$defs": {
            "Outcome": {
                "description": "Referenced outcome",
                "type": "object",
                "properties": {"detail": {"type": "string"}},
                "required": ["detail"]
            }
        },
        "$ref": "#/$defs/Outcome",
        "unevaluatedProperties": false
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();
    let candidate = json!({"detail": "ok"});

    assert!(schema.is_valid(&candidate));
    let validator = wrapper_validator(&transport.native_parameters);
    assert!(validator.is_valid(&json!({"result": candidate})));
}

#[test]
fn regex_projection_preserves_referenced_pointer_targets() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "patternProperties": {
            "^x-": {
                "$defs": {
                    "Detail": {
                        "type": "object",
                        "properties": {"summary": {"type": "string"}},
                        "required": ["summary"],
                        "additionalProperties": false
                    }
                },
                "type": "object",
                "properties": {
                    "detail": {
                        "$ref": "#/patternProperties/%5Ex-/$defs/Detail"
                    }
                },
                "required": ["detail"],
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    });
    let schema = retained(&serde_json::to_vec(&document).unwrap());
    let transport = derive_transport_schema(&schema).unwrap();
    let candidate = json!({"x-item": {"detail": {"summary": "ok"}}});

    assert!(schema.is_valid(&candidate));
    let validator = Validator::options()
        .with_draft(Draft::Draft202012)
        .with_pattern_options(PatternOptions::regex())
        .with_retriever(RejectRetrieval)
        .build(&transport.native_parameters)
        .expect("the generated model-facing schema must not contain dangling references");
    assert!(validator.is_valid(&json!({"result": candidate})));
}

#[test]
fn fixed_identity_and_schema_materialize_the_checked_extension_bytes() {
    let schema = retained(RETAINED_FIXTURE);
    let transport = derive_transport_schema(&schema).unwrap();
    let input: Value = serde_json::from_str(MATERIALIZATION_INPUT).unwrap();
    assert_eq!(transport.native_parameters, input["parameters"]);

    let config = ExtensionConfig {
        tool_name: input["toolName"].as_str().unwrap(),
        socket_path: input["socketPath"].as_str().unwrap(),
        parameters: &transport.native_parameters,
    };
    let first = materialize_extension(&config).unwrap();
    let second = materialize_extension(&config).unwrap();
    assert_eq!(first, second);
    assert_eq!(first, GENERATED_EXTENSION);

    let fixed = result_tool_name(&fixed_identity("run-fixed", "step-fixed")).unwrap();
    let repeated = result_tool_name(&fixed_identity("run-fixed", "step-fixed")).unwrap();
    let distinct = result_tool_name(&fixed_identity("run-distinct", "step-fixed")).unwrap();
    assert_eq!(fixed, repeated);
    assert_ne!(fixed, distinct);
    assert_ne!(
        socket_alias_directory(&fixed).unwrap(),
        socket_alias_directory(&distinct).unwrap()
    );
    assert!(fixed.starts_with(TOOL_NAME_PREFIX));
    assert!(
        fixed
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_' || byte.is_ascii_digit())
    );
}

#[tokio::test]
async fn socket_accepts_one_exact_bounded_request_and_returns_one_response() {
    let temporary = tempfile::tempdir().unwrap();
    let request = request_bytes("call-fixed", "scherzo_result_fixed", json!({"answer": 42}));
    let maximum = NonZeroU64::new(u64::try_from(request.len()).unwrap()).unwrap();
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-exact");
    let mut server =
        ResultSocketServer::start(listener, maximum, receive_deadline(), TestClock::Pending);
    let client = tokio::spawn(exchange(socket_address, framed(&request)));

    let ResultSocketEvent::Request(incoming) = server.receive().await else {
        panic!("exact request must reach the validator");
    };
    assert_eq!(incoming.request().tool_call_id(), "call-fixed");
    assert_eq!(incoming.request().candidate(), Some(&json!({"answer": 42})));
    incoming
        .respond(ValidatePiResultV1Response::valid())
        .await
        .unwrap();
    assert_eq!(client.await.unwrap(), json!({"kind": "Valid"}));
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
}

#[tokio::test]
async fn delayed_second_frame_on_one_connection_is_fatal() {
    let temporary = tempfile::tempdir().unwrap();
    let first = request_bytes("call-first", "scherzo_result_fixed", json!({"answer": 1}));
    let second = request_bytes("call-second", "scherzo_result_fixed", json!({"answer": 2}));
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-delayed-second-frame");
    let mut server = ResultSocketServer::start(
        listener,
        NonZeroU64::new(1024).unwrap(),
        receive_deadline(),
        TestClock::Pending,
    );
    let mut client = UnixStream::connect(socket_address).await.unwrap();
    client.write_all(&framed(&first)).await.unwrap();

    let ResultSocketEvent::Request(incoming) = server.receive().await else {
        panic!("the first request must reach validation");
    };
    client.write_all(&framed(&second)).await.unwrap();
    // The protocol failure is allowed to discard the validator's pending success.
    let _ = incoming.respond(ValidatePiResultV1Response::valid()).await;

    let mut response = Vec::new();
    let read = client.read_to_end(&mut response).await;
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();

    assert!(
        read.is_ok(),
        "a delayed second request must receive Fatal instead of a connection reset: {read:?}"
    );
    assert!(response.len() >= 4);
    assert_eq!(
        serde_json::from_slice::<Value>(&response[4..]).unwrap(),
        json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE}),
        "a second request queued before the first response must invalidate the connection"
    );
}

#[tokio::test]
async fn delayed_oversized_second_frame_returns_framed_fatal() {
    let temporary = tempfile::tempdir().unwrap();
    let first = request_bytes("call-first", "scherzo_result_fixed", json!({"answer": 1}));
    let second = request_bytes(
        "call-second",
        "scherzo_result_fixed",
        json!({"answer": "x".repeat(4096)}),
    );
    let maximum = NonZeroU64::new(1024).unwrap();
    assert!(u64::try_from(first.len()).unwrap() <= maximum.get());
    assert!(u64::try_from(second.len()).unwrap() > maximum.get());
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-delayed-oversized-second-frame");
    let mut server =
        ResultSocketServer::start(listener, maximum, receive_deadline(), TestClock::Pending);
    let mut client = UnixStream::connect(socket_address).await.unwrap();
    client.write_all(&framed(&first)).await.unwrap();

    let ResultSocketEvent::Request(incoming) = server.receive().await else {
        panic!("the first request must reach validation");
    };
    client.write_all(&framed(&second)).await.unwrap();
    let _ = incoming.respond(ValidatePiResultV1Response::valid()).await;

    let mut response = Vec::new();
    let read = client.read_to_end(&mut response).await;
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();

    assert!(
        read.is_ok(),
        "a delayed oversized request must receive Fatal instead of a connection reset: {read:?}"
    );
    assert!(response.len() >= 4);
    assert_eq!(
        serde_json::from_slice::<Value>(&response[4..]).unwrap(),
        json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
    );
}

#[tokio::test]
async fn oversized_malformed_and_multi_frame_connections_are_fatal() {
    for (malformed, maximum) in [
        (65_u32.to_be_bytes().to_vec(), 64_u64),
        (framed(b"not-json"), 1024),
        (
            framed(
                br#"{"kind":"ValidatePiResultV1","toolCallId":"call-fixed","toolName":"scherzo_result_fixed","arguments":{"result":{"decision":"recheck","decision":"gave_up"}}}"#,
            ),
            1024,
        ),
        (
            {
                let mut frame = framed(&request_bytes(
                    "call-fixed",
                    "scherzo_result_fixed",
                    json!(true),
                ));
                frame.push(0);
                frame
            },
            1024,
        ),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let (listener, socket_address, socket_path, alias_directory) =
            bind_test_socket(temporary.path(), "socket-malformed");
        let mut server = ResultSocketServer::start(
            listener,
            NonZeroU64::new(maximum).unwrap(),
            receive_deadline(),
            TestClock::Pending,
        );
        let client = tokio::spawn(exchange(socket_address, malformed));

        assert!(matches!(
            server.receive().await,
            ResultSocketEvent::ProtocolFailure
        ));
        assert_eq!(
            client.await.unwrap(),
            json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
        );
        server.shutdown().await.unwrap();
        remove_materialized_file(&socket_path).unwrap();
        remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
    }
}

#[tokio::test]
async fn incomplete_frame_reaches_the_receive_deadline() {
    let temporary = tempfile::tempdir().unwrap();
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-incomplete");
    let (registrations, mut registered_deadlines) = tokio::sync::mpsc::unbounded_channel();
    let (release, release_receiver) = tokio::sync::watch::channel(false);
    let mut server = ResultSocketServer::start(
        listener,
        NonZeroU64::new(1024).unwrap(),
        receive_deadline(),
        TestClock::Controlled {
            registrations,
            release: release_receiver,
        },
    );
    let mut client = UnixStream::connect(socket_address).await.unwrap();
    client.write_all(&16_u32.to_be_bytes()).await.unwrap();
    client.write_all(b"{").await.unwrap();

    assert_eq!(
        registered_deadlines.recv().await,
        Some(Duration::from_secs(5))
    );
    assert_eq!(
        registered_deadlines.recv().await,
        Some(Duration::from_secs(5))
    );
    release.send_replace(true);
    assert!(matches!(
        server.receive().await,
        ResultSocketEvent::ProtocolFailure
    ));

    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert!(response.len() >= 4);
    assert_eq!(
        serde_json::from_slice::<Value>(&response[4..]).unwrap(),
        json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
    );
    server.shutdown().await.unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, not a behavior assertion"
)]
#[tokio::test]
async fn shutdown_quiesces_with_queued_protocol_failures() {
    let temporary = tempfile::tempdir().unwrap();
    let (listener, socket_address, socket_path, alias_directory) =
        bind_test_socket(temporary.path(), "socket-queued-failures");
    let server = ResultSocketServer::start(
        listener,
        NonZeroU64::new(1024).unwrap(),
        receive_deadline(),
        TestClock::Pending,
    );

    for _ in 0..2 {
        assert_eq!(
            exchange(socket_address.clone(), framed(b"not-json")).await,
            json!({"kind": "Fatal", "cause": CHANNEL_FAILURE_CAUSE})
        );
    }

    tokio::time::timeout(Duration::from_secs(10), server.shutdown())
        .await
        .expect("queued protocol failures must not block shutdown")
        .unwrap();
    remove_materialized_file(&socket_path).unwrap();
    remove_socket_alias(&alias_directory, &alias_directory.join(SOCKET_ALIAS_NAME)).unwrap();
}

#[tokio::test]
async fn prepared_socket_and_extension_are_private_until_explicit_quiescence() {
    let temporary = tempfile::tempdir().unwrap();
    fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let bridge = PreparedResultBridge::prepare(
        &fixed_identity("run-fixed", "step-fixed"),
        temporary.path(),
        &retained(RETAINED_FIXTURE),
        PiJsonV1ProtocolLimits::profile(),
        receive_deadline(),
        TestClock::Pending,
    )
    .unwrap();
    let extension_path = bridge.extension_path().to_owned();
    let socket_path = bridge.socket_path.clone();
    let alias_directory = bridge.socket_alias_directory.clone();
    let socket_address = alias_directory
        .join(SOCKET_ALIAS_NAME)
        .join(SOCKET_FILE_NAME);
    let fixed_extension_bytes = fs::read(&extension_path).unwrap();

    assert!(extension_path.exists());
    assert!(socket_path.exists());
    assert_eq!(
        fs::metadata(&extension_path).unwrap().permissions().mode() & 0o777,
        0o400
    );
    assert_eq!(
        fs::metadata(&socket_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&alias_directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(socket_address.to_string_lossy().len() < 100);
    bridge.shutdown().await.unwrap();
    assert!(!extension_path.exists());
    assert!(!socket_path.exists());
    assert!(!alias_directory.exists());

    let second_temporary = tempfile::tempdir().unwrap();
    fs::set_permissions(second_temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let second = PreparedResultBridge::prepare(
        &fixed_identity("run-fixed", "step-fixed"),
        second_temporary.path(),
        &retained(RETAINED_FIXTURE),
        PiJsonV1ProtocolLimits::profile(),
        receive_deadline(),
        TestClock::Pending,
    )
    .unwrap();
    assert_eq!(
        fs::read(second.extension_path()).unwrap(),
        fixed_extension_bytes
    );
    second.shutdown().await.unwrap();
}

#[test]
fn fragment_only_authored_root_id_preserves_standalone_semantics() {
    let document = json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "$id": "#",
        "type": "integer"
    });
    let retained_bytes = serde_json::to_vec(&document).unwrap();
    let schema = retained(&retained_bytes);
    let standalone = wrapper_validator(schema.document());
    let transport = derive_transport_schema(&schema).unwrap();
    let wrapped = wrapper_validator(&transport.complete_wrapper);

    assert!(standalone.is_valid(&json!(1)));
    assert_eq!(
        wrapped.is_valid(&json!({"result": 1})),
        standalone.is_valid(&json!(1)),
        "the native wrapper must accept every candidate accepted by the authored schema"
    );
}

fn request_bytes(call_id: &str, tool_name: &str, result: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "kind": "ValidatePiResultV1",
        "toolCallId": call_id,
        "toolName": tool_name,
        "arguments": {"result": result}
    }))
    .unwrap()
}

fn framed(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn bind_test_socket(target: &Path, run: &str) -> (UnixListener, PathBuf, PathBuf, PathBuf) {
    let tool_name = result_tool_name(&fixed_identity(run, "socket-test")).unwrap();
    let alias_directory = socket_alias_directory(&tool_name).unwrap();
    let alias = alias_directory.join(SOCKET_ALIAS_NAME);
    create_socket_alias(&alias_directory, &alias, target).unwrap();
    let socket_address = alias.join(SOCKET_FILE_NAME);
    let listener = UnixListener::bind(&socket_address).unwrap();
    (
        listener,
        socket_address,
        target.join(SOCKET_FILE_NAME),
        alias_directory,
    )
}

async fn exchange(socket_address: PathBuf, request: Vec<u8>) -> Value {
    let mut stream = UnixStream::connect(socket_address).await.unwrap();
    stream.write_all(&request).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(response.len() >= 4);
    let payload_length = u32::from_be_bytes(response[..4].try_into().unwrap());
    assert_eq!(usize::try_from(payload_length).unwrap(), response.len() - 4);
    serde_json::from_slice(&response[4..]).unwrap()
}
