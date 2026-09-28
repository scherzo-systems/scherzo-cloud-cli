use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    schema_version: u8,
    multiplication_cases: Vec<MultiplicationCase>,
    bound_cases: Vec<BoundCase>,
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultiplicationCase {
    name: String,
    left: u64,
    right: u64,
    expected: Option<u64>,
    #[serde(default)]
    overflow: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BoundCase {
    name: String,
    contract: String,
    finalizers: u64,
    maximum_transitions: u64,
    admit: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
    name: String,
    steps: u64,
    finalizers: u64,
    recovery_rounds: u64,
    handler_rounds: u64,
    expected: Option<Expected>,
    expected_general_maximum_transitions: Option<u64>,
    expected_cloud_maximum_transitions: Option<u64>,
    expected_failure: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    general_maximum_transitions: u64,
    cloud_maximum_transitions: u64,
    maximum_invocations: u64,
    maximum_retained_bytes_per_invocation: u64,
    diagnostic_retention_bytes: u64,
    native_session_retention_bytes: u64,
    aggregate_retention_bytes: u64,
    condition_transition_count: u64,
    aggregate_condition_transition_bytes: u64,
    terminal_result_structure_bytes: u64,
    portable_result_bytes: u64,
    encoded_outbox_bytes: u64,
}

#[test]
fn cloud_capacity_limits_match_shared_contract() {
    let contract: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/workflow/v1/capacity-contract.json"
    )))
    .unwrap();
    let number = |path: &[&str]| -> u64 {
        path.iter()
            .fold(&contract, |value, key| &value[*key])
            .as_u64()
            .unwrap()
    };
    assert_eq!(
        GENERAL_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS,
        number(&["transitionBounds", "general", "maximumWithoutFinalizers"])
    );
    assert_eq!(
        GENERAL_MAXIMUM_TRANSITIONS_WITH_FINALIZERS,
        number(&["transitionBounds", "general", "maximumWithFinalizers"])
    );
    assert_eq!(
        CLOUD_MAXIMUM_TRANSITIONS_WITHOUT_FINALIZERS,
        number(&["transitionBounds", "selected", "maximumWithoutFinalizers"])
    );
    assert_eq!(
        CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS,
        number(&["transitionBounds", "selected", "maximumWithFinalizers"])
    );
    assert_eq!(number(&["transitionBounds", "general", "minimum"]), 1);
    assert_eq!(number(&["transitionBounds", "selected", "minimum"]), 1);
    assert_eq!(
        number(&["commonBounds", "maximumInvocations", "minimum"]),
        1
    );
    assert_eq!(
        number(&[
            "commonBounds",
            "maximumRetainedBytesPerInvocation",
            "minimum"
        ]),
        1
    );
    assert_eq!(
        number(&["cancellationGraceSeconds", "minimum"]),
        super::super::cancellation::MINIMUM_CANCELLATION_GRACE.as_secs()
    );
    assert_eq!(
        RUNNER_OBSERVATION_RESERVE,
        number(&["formulas", "runnerObservationReserve"])
    );
    assert_eq!(
        RUNNER_ORDINARY_FRAME_BYTES,
        number(&["formulas", "runnerOrdinaryFrameBytes"])
    );
    assert_eq!(
        RUNNER_TERMINAL_FRAME_BYTES,
        number(&[
            "variants",
            "ordinary",
            "terminalResultStructureBytes",
            "exact"
        ])
    );
    assert_eq!(
        ORDINARY_PORTABLE_RESULT_BYTES,
        number(&["variants", "ordinary", "portableResultBytes", "exact"])
    );
    assert_eq!(
        MAXIMUM_CONDITION_TRANSITION_COUNT,
        number(&[
            "variants",
            "conditional",
            "conditionTransitionCount",
            "maximum"
        ])
    );
    assert_eq!(
        MAXIMUM_CONDITION_TRANSITION_BYTES,
        number(&[
            "variants",
            "conditional",
            "aggregateConditionTransitionBytes",
            "maximum"
        ])
    );
    assert_eq!(
        MAXIMUM_TERMINAL_RESULT_STRUCTURE_BYTES,
        number(&[
            "variants",
            "conditional",
            "terminalResultStructureBytes",
            "maximum"
        ])
    );
    assert_eq!(
        MAXIMUM_PORTABLE_RESULT_BYTES,
        number(&["variants", "conditional", "portableResultBytes", "maximum"])
    );
    assert_eq!(
        MAXIMUM_ENCODED_OUTBOX_BYTES,
        number(&["variants", "conditional", "encodedOutboxBytes", "maximum"])
    );
    let overhead = super::super::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES
        + super::super::result_metadata::MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES;
    assert_eq!(
        overhead,
        number(&["formulas", "portableResultStructureOverheadBytes"])
    );
    assert_eq!(
        (CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS + RUNNER_OBSERVATION_RESERVE)
            * RUNNER_ORDINARY_FRAME_BYTES
            + RUNNER_TERMINAL_FRAME_BYTES
            - RUNNER_ORDINARY_FRAME_BYTES,
        number(&["variants", "ordinary", "encodedOutboxBytes", "maximum"])
    );
    for (path, value) in [
        ("conditionTransitionCount", 1),
        ("aggregateConditionTransitionBytes", 1),
        ("terminalResultStructureBytes", 2),
        ("portableResultBytes", overhead + 2),
        ("encodedOutboxBytes", 3),
    ] {
        assert_eq!(number(&["variants", "conditional", path, "minimum"]), value);
    }
    assert_eq!(
        number(&[
            "variants",
            "ordinary",
            "aggregateConditionTransitionBytes",
            "exact"
        ]),
        0
    );
    assert_eq!(
        super::super::cancellation::MAXIMUM_CANCELLATION_GRACE.as_secs(),
        number(&["cancellationGraceSeconds", "maximum"])
    );
    assert_eq!(
        super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
        number(&[
            "commonBounds",
            "maximumRetainedBytesPerInvocation",
            "maximum"
        ])
    );
    let budget = super::super::admission::WorkflowCapacityBudget::supported_maximum();
    assert_eq!(
        budget.maximum_invocations,
        number(&["commonBounds", "maximumInvocations", "maximum"])
    );
    assert_eq!(
        budget.diagnostic_retention_bytes,
        number(&["commonBounds", "diagnosticRetentionBytes", "maximum"])
    );
    assert_eq!(
        budget.native_session_retention_bytes,
        number(&["commonBounds", "nativeSessionRetentionBytes", "maximum"])
    );
    assert_eq!(
        budget.aggregate_retention_bytes,
        number(&["commonBounds", "aggregateRetentionBytes", "maximum"])
    );
    assert_eq!(
        budget.encoded_outbox_bytes,
        number(&["variants", "conditional", "encodedOutboxBytes", "maximum"])
    );
}

#[test]
fn condition_capacity_replay_and_runner_share_the_numerical_boundary() {
    let ordinary = ConditionCapacityBounds {
        selected_maximum_transitions: 7,
        condition_transition_count: 0,
        aggregate_condition_transition_bytes: 0,
        terminal_result_structure_bytes: RUNNER_TERMINAL_FRAME_BYTES,
        portable_result_bytes: ORDINARY_PORTABLE_RESULT_BYTES,
        encoded_outbox_bytes: 85_458_944,
    };
    assert!(valid_condition_capacity(ordinary));
    assert!(!valid_condition_capacity(ConditionCapacityBounds {
        encoded_outbox_bytes: ordinary.encoded_outbox_bytes + 1,
        ..ordinary
    }));

    let conditional = ConditionCapacityBounds {
        condition_transition_count: 1,
        aggregate_condition_transition_bytes: RUNNER_ORDINARY_FRAME_BYTES,
        terminal_result_structure_bytes: 2 * RUNNER_ORDINARY_FRAME_BYTES,
        portable_result_bytes: 2 * RUNNER_ORDINARY_FRAME_BYTES
            + super::super::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES
            + super::super::result_metadata::MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES,
        encoded_outbox_bytes: 72 * RUNNER_ORDINARY_FRAME_BYTES,
        ..ordinary
    };
    assert!(valid_condition_capacity(conditional));
    assert!(!valid_condition_capacity(ConditionCapacityBounds {
        condition_transition_count: 257,
        ..conditional
    }));
    assert!(!valid_condition_capacity(ConditionCapacityBounds {
        selected_maximum_transitions: u64::MAX,
        ..conditional
    }));
}

#[test]
fn shared_recovery_capacity_vectors_match_the_resolver_owned_calculation() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/workflow/v1/recovery-capacity-vectors.json");
    let fixture: Fixture = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(fixture.schema_version, 1);

    for case in fixture.multiplication_cases {
        let actual = checked_product(case.left, case.right);
        if case.overflow {
            assert_eq!(
                actual,
                Err(CapacityCalculationFailure::ArithmeticOverflow),
                "multiplication case {}",
                case.name
            );
        } else {
            assert_eq!(
                actual.ok(),
                case.expected,
                "multiplication case {}",
                case.name
            );
        }
    }

    for case in fixture.bound_cases {
        let result = match case.contract.as_str() {
            "general" => {
                validate_general_transition_bound(case.maximum_transitions, case.finalizers)
            }
            "workflow_v1_cloud_inputs_artifacts@1" => {
                validate_cloud_transition_bound(case.maximum_transitions, case.finalizers)
            }
            _ => panic!("unknown capacity contract in {}", case.name),
        };
        assert_eq!(result.is_ok(), case.admit, "bound case {}", case.name);
    }

    for vector in fixture.vectors {
        let counts = CapacityCounts {
            steps: vector.steps,
            finalizers: vector.finalizers,
            recovery_rounds: vector.recovery_rounds,
            handler_rounds: vector.handler_rounds,
        };
        match (vector.expected, vector.expected_failure.as_deref()) {
            (Some(expected), None) => {
                let actual = calculate_capacity(counts).unwrap();
                assert_eq!(
                    actual,
                    ComputedWorkflowCapacity {
                        general_maximum_transitions: expected.general_maximum_transitions,
                        cloud_maximum_transitions: expected.cloud_maximum_transitions,
                        maximum_invocations: expected.maximum_invocations,
                        maximum_retained_bytes_per_invocation: expected
                            .maximum_retained_bytes_per_invocation,
                        diagnostic_retention_bytes: expected.diagnostic_retention_bytes,
                        native_session_retention_bytes: expected.native_session_retention_bytes,
                        aggregate_retention_bytes: expected.aggregate_retention_bytes,
                        condition_transition_count: expected.condition_transition_count,
                        aggregate_condition_transition_bytes: expected
                            .aggregate_condition_transition_bytes,
                        terminal_result_structure_bytes: expected.terminal_result_structure_bytes,
                        portable_result_bytes: expected.portable_result_bytes,
                        encoded_outbox_bytes: expected.encoded_outbox_bytes,
                    },
                    "vector {}",
                    vector.name
                );
            }
            (None, Some("general_transition_capacity_exceeded")) => {
                let (_, general, cloud) = transition_bounds(counts).unwrap();
                assert_eq!(
                    Some(general),
                    vector.expected_general_maximum_transitions,
                    "general vector {}",
                    vector.name
                );
                assert_eq!(
                    Some(cloud),
                    vector.expected_cloud_maximum_transitions,
                    "cloud vector {}",
                    vector.name
                );
                assert_eq!(
                    validate_general_transition_bound(general, counts.finalizers),
                    Err(CapacityCalculationFailure::GeneralTransitionCapacityExceeded),
                    "general cap vector {}",
                    vector.name
                );
                assert_eq!(
                    validate_cloud_transition_bound(cloud, counts.finalizers),
                    Err(CapacityCalculationFailure::CloudTransitionCapacityExceeded),
                    "cloud cap vector {}",
                    vector.name
                );
                assert_eq!(
                    calculate_capacity(counts),
                    Err(CapacityCalculationFailure::GeneralTransitionCapacityExceeded),
                    "vector {}",
                    vector.name
                );
            }
            (None, Some("arithmetic_overflow")) => assert_eq!(
                calculate_capacity(counts),
                Err(CapacityCalculationFailure::ArithmeticOverflow),
                "vector {}",
                vector.name
            ),
            _ => panic!("invalid capacity vector {}", vector.name),
        }
    }
}
