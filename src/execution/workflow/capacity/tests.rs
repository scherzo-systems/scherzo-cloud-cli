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
    encoded_outbox_bytes: u64,
}

#[test]
fn condition_evidence_capacity_derives_every_frame_and_result_class() {
    let maximum =
        calculate_condition_evidence_capacity(super::super::resolution::MAX_SOURCE_CLOSURE_BYTES)
            .unwrap();
    assert_eq!(
        maximum,
        ConditionEvidenceCapacity {
            maximum_json_escaped_pointer_bytes: 201_326_592,
            condition_transition_bytes: MAXIMUM_CONDITION_TRANSITION_BYTES,
            terminal_result_structure_bytes: MAXIMUM_TERMINAL_RESULT_STRUCTURE_BYTES,
            portable_result_bytes: MAXIMUM_PORTABLE_RESULT_BYTES,
        }
    );
    assert_eq!(
        maximum.portable_result_bytes,
        maximum.terminal_result_structure_bytes
            + super::super::result_metadata::MAXIMUM_ENCODED_RETAINED_STREAM_BYTES
            + super::super::result_metadata::MAXIMUM_EXPORT_MEDIA_TYPE_JSON_BYTES
    );

    assert_eq!(
        calculate_condition_evidence_capacity(
            super::super::resolution::MAX_SOURCE_CLOSURE_BYTES + 1
        ),
        Err(ConditionCapacityFailure::SourceClosureCapacityExceeded)
    );
    for source_bytes in [u64::MAX, u64::MAX / 3, u64::MAX / 8 + 1] {
        assert_eq!(
            calculate_condition_evidence_capacity(source_bytes),
            Err(ConditionCapacityFailure::ArithmeticOverflow)
        );
    }
    assert_eq!(
        portable_result_bound(u64::MAX),
        Err(ConditionCapacityFailure::ArithmeticOverflow)
    );
    assert_eq!(condition_false_transition_bound(), Ok(33_871));

    for source_bytes in [0, 1, 1_024, 1024 * 1024] {
        let capacity = calculate_condition_evidence_capacity(source_bytes).unwrap();
        assert_eq!(
            capacity.maximum_json_escaped_pointer_bytes,
            source_bytes * JSON_ESCAPED_POINTER_SOURCE_MULTIPLIER
        );
        let expected_transition_bytes = (source_bytes * 4).max(33_871);
        assert_eq!(
            capacity.condition_transition_bytes,
            expected_transition_bytes
        );
        assert_eq!(
            capacity.terminal_result_structure_bytes,
            expected_transition_bytes * 2
        );
    }
}

#[test]
fn condition_evidence_capacity_reserves_exact_outbox_classes() {
    let entries = 10 + RUNNER_OBSERVATION_RESERVE;
    let expected = (entries - 2 - 1) * RUNNER_ORDINARY_FRAME_BYTES + 1_000 + 2_000;
    assert_eq!(
        calculate_condition_outbox_reservation(10, 2, 1_000, 2_000),
        Ok(expected)
    );

    let baseline = calculate_condition_outbox_reservation(
        CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS,
        0,
        0,
        RUNNER_TERMINAL_FRAME_BYTES,
    )
    .unwrap();
    assert_eq!(
        baseline,
        (CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS + RUNNER_OBSERVATION_RESERVE)
            * RUNNER_ORDINARY_FRAME_BYTES
            + RUNNER_TERMINAL_FRAME_BYTES
            - RUNNER_ORDINARY_FRAME_BYTES
    );

    let maximum =
        calculate_condition_evidence_capacity(super::super::resolution::MAX_SOURCE_CLOSURE_BYTES)
            .unwrap();
    assert!(
        calculate_condition_outbox_reservation(
            CLOUD_MAXIMUM_TRANSITIONS_WITH_FINALIZERS,
            256,
            maximum.condition_transition_bytes,
            maximum.terminal_result_structure_bytes,
        )
        .is_ok()
    );
    assert_eq!(
        calculate_condition_outbox_reservation(0, RUNNER_OBSERVATION_RESERVE, 0, 0),
        Err(ConditionCapacityFailure::OutboxEntryCapacityExceeded)
    );
    assert_eq!(
        calculate_condition_outbox_reservation(u64::MAX, 0, 0, 0),
        Err(ConditionCapacityFailure::ArithmeticOverflow)
    );
    assert_eq!(
        calculate_condition_outbox_reservation(1, 0, u64::MAX, 1),
        Err(ConditionCapacityFailure::ArithmeticOverflow)
    );
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
