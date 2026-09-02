use std::sync::Arc;

use serde_json::{Value, json};

use super::*;
use crate::execution::workflow::decode;

fn pointer(authored: &str) -> JsonPointer {
    JsonPointer::parse(Arc::<str>::from(authored)).unwrap()
}

fn captured_json(source: &str) -> CapturedJson {
    CapturedJson::fixture(Arc::new(serde_json::from_str(source).unwrap()))
}

fn node(id: &str) -> WorkflowNode {
    WorkflowNode {
        id: id.to_owned(),
        role: super::super::validated::WorkflowNodeRole::Step,
    }
}

fn json_literal(value: Value) -> ResolvedOperand {
    ResolvedOperand::json_literal(Arc::new(value))
}

#[test]
fn condition_pointer_round_trips_and_selects_strict_tokens() {
    let document = json!({
        "a/b": {"m~n": 7},
        "": null,
        "0": "object zero",
        "01": "object leading zero",
        "array": [null, {"": true}]
    });
    for (authored, tokens, selected) in [
        ("", Vec::<&str>::new(), Some(document.clone())),
        ("/a~1b/m~0n", vec!["a/b", "m~n"], Some(json!(7))),
        ("/", vec![""], Some(Value::Null)),
        ("/0", vec!["0"], Some(json!("object zero"))),
        ("/01", vec!["01"], Some(json!("object leading zero"))),
        ("/array/0", vec!["array", "0"], Some(Value::Null)),
        ("/array/1/", vec!["array", "1", ""], Some(json!(true))),
        ("/array/01", vec!["array", "01"], None),
        ("/array/-", vec!["array", "-"], None),
        ("/array/2", vec!["array", "2"], None),
    ] {
        let parsed = pointer(authored);
        assert_eq!(parsed.authored(), authored);
        assert_eq!(parsed.tokens().collect::<Vec<_>>(), tokens);
        match (parsed.select(&document), selected) {
            (JsonSelection::Selected(actual), Some(expected)) => assert_eq!(actual, &expected),
            (JsonSelection::Missing, None) => {}
            (actual, expected) => {
                panic!("selection {authored:?} = {actual:?}, expected {expected:?}")
            }
        }
    }

    let array = json!(["zero", "one"]);
    assert_eq!(
        pointer("/0").select(&array),
        JsonSelection::Selected(&json!("zero"))
    );
    for authored in ["/01", "/-", "/184467440737095516160"] {
        assert_eq!(pointer(authored).select(&array), JsonSelection::Missing);
    }
}

#[test]
fn condition_pointer_rejects_non_pointer_and_invalid_escapes() {
    for authored in ["x", "#", "#/a", "/~", "/~2", "/a/~x"] {
        assert_eq!(
            JsonPointer::parse(Arc::<str>::from(authored)),
            Err(InvalidJsonPointer)
        );
    }
    assert_eq!(pointer("/~01").tokens().collect::<Vec<_>>(), ["~1"]);
}

#[test]
fn condition_equality_preserves_text_and_json_value_semantics() {
    for (left, right, equal) in [
        ("exact", "exact", true),
        ("Exact", "exact", false),
        ("line\n", "line\r\n", false),
        ("é", "e\u{301}", false),
        (" value", "value", false),
    ] {
        assert_eq!(
            operands_equal(SelectedOperand::Text(left), SelectedOperand::Text(right)),
            equal
        );
    }

    for (left, right, equal) in [
        (
            r#"{"a":1,"b":[true,null]}"#,
            r#"{"b":[true,null],"a":1.0}"#,
            true,
        ),
        (r#"[1,2]"#, r#"[2,1]"#, false),
        (r#""exact""#, r#""Exact""#, false),
        ("true", "1", false),
        ("null", "false", false),
        ("1", "1.0", true),
        ("1.0", "1e0", true),
        ("-0", "0e999999999999999999999999", true),
        ("1000e-3", "1", true),
        ("-1200.00e-2", "-12", true),
        (
            "1e999999999999999999999999",
            "10e999999999999999999999998",
            true,
        ),
        (
            "1e-999999999999999999999999",
            "10e-1000000000000000000000000",
            true,
        ),
        (
            "1e999999999999999999999999",
            "1e999999999999999999999998",
            false,
        ),
    ] {
        let left: Value = serde_json::from_str(left).unwrap();
        let right: Value = serde_json::from_str(right).unwrap();
        assert_eq!(
            json_semantically_equal(&left, &right),
            equal,
            "{left} and {right}"
        );
    }

    let predicate = ResolvedPredicate::Equals([
        ResolvedOperand::text_literal("same"),
        json_literal(json!("same")),
    ]);
    assert!(matches!(
        evaluate(
            &predicate,
            &ConditionValues::default(),
            &ConditionDispositions::default()
        ),
        ConditionEvaluation::False { .. }
    ));
}

#[test]
fn condition_evaluation_is_left_to_right_and_completion_ordered() {
    let text = CapturedText::new(Arc::from("yes"));
    let json = captured_json(r#"{"n":1}"#);
    let mut values = ConditionValues::default();
    values.insert_text("imports.prompt", &text);
    values.insert_json("outputs.plan.result", &json);
    let verify = node("verify");
    let mut dispositions = ConditionDispositions::default();
    dispositions.insert(verify.clone(), TerminalDisposition::Succeeded);

    let predicate = ResolvedPredicate::All(
        vec![
            ResolvedPredicate::Equals([
                ResolvedOperand::text_reference("imports.prompt"),
                ResolvedOperand::text_literal("yes"),
            ]),
            ResolvedPredicate::Any(
                vec![
                    ResolvedPredicate::Exists(ResolvedSelector::new(
                        "outputs.plan.result",
                        pointer("/missing"),
                    )),
                    ResolvedPredicate::Not(Box::new(ResolvedPredicate::Equals([
                        ResolvedOperand::json_reference("outputs.plan.result", Some(pointer("/n"))),
                        json_literal(json!(2)),
                    ]))),
                ]
                .into(),
            ),
            ResolvedPredicate::Disposition {
                node: verify,
                is: TerminalDisposition::Failed,
            },
            ResolvedPredicate::Equals([
                ResolvedOperand::json_reference("short.circuited", Some(pointer("/missing"))),
                json_literal(Value::Null),
            ]),
        ]
        .into(),
    );

    let ConditionEvaluation::False {
        evaluated_predicates,
    } = evaluate(&predicate, &values, &dispositions)
    else {
        panic!("the false root should retain its trace");
    };
    assert_eq!(
        evaluated_predicates
            .iter()
            .map(|entry| (entry.path.as_str(), entry.result))
            .collect::<Vec<_>>(),
        [
            ("/all/0", true),
            ("/all/1/any/0", false),
            ("/all/1/any/1/not", false),
            ("/all/1/any/1", true),
            ("/all/1", true),
            ("/all/2", false),
            ("", false),
        ]
    );
    assert_eq!(
        values
            .accessed_references()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        [
            "imports.prompt",
            "outputs.plan.result",
            "outputs.plan.result"
        ]
    );
}

#[test]
fn condition_evaluation_short_circuits_without_retaining_a_true_trace() {
    let text = CapturedText::new(Arc::from("yes"));
    let mut values = ConditionValues::default();
    values.insert_text("imports.prompt", &text);
    let predicate = ResolvedPredicate::Any(
        vec![
            ResolvedPredicate::Equals([
                ResolvedOperand::text_reference("imports.prompt"),
                ResolvedOperand::text_literal("yes"),
            ]),
            ResolvedPredicate::Equals([
                ResolvedOperand::json_reference("unevaluated", Some(pointer("/missing"))),
                json_literal(Value::Null),
            ]),
        ]
        .into(),
    );

    assert_eq!(
        evaluate(&predicate, &values, &ConditionDispositions::default()),
        ConditionEvaluation::Passed
    );
    assert_eq!(
        values
            .accessed_references()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        ["imports.prompt"]
    );
}

#[test]
fn condition_evaluation_missing_pointer_and_unavailable_source_are_distinct() {
    let json = captured_json(r#"{"present":null}"#);
    let mut values = ConditionValues::default();
    values.insert_json("outputs.plan.result", &json);

    let missing = ResolvedPredicate::Equals([
        ResolvedOperand::json_reference("outputs.plan.result", Some(pointer("/missing~01"))),
        ResolvedOperand::json_reference("unevaluated", Some(pointer("/also-missing"))),
    ]);
    assert_eq!(
        evaluate(&missing, &values, &ConditionDispositions::default()),
        ConditionEvaluation::Failed {
            canonical_ref: Arc::from("outputs.plan.result"),
            pointer: Arc::from("/missing~01"),
        }
    );
    assert_eq!(
        values
            .accessed_references()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        ["outputs.plan.result"]
    );

    let unavailable = ResolvedPredicate::Exists(ResolvedSelector::new(
        "outputs.unavailable.result",
        pointer("/present"),
    ));
    assert_eq!(
        evaluate(&unavailable, &values, &ConditionDispositions::default()),
        ConditionEvaluation::Unavailable {
            input: UnavailableConditionInput::Value {
                canonical_ref: Arc::from("outputs.unavailable.result")
            }
        }
    );

    let exists = ResolvedPredicate::Exists(ResolvedSelector::new(
        "outputs.plan.result",
        pointer("/present"),
    ));
    assert_eq!(
        evaluate(&exists, &values, &ConditionDispositions::default()),
        ConditionEvaluation::Passed
    );
}

#[test]
fn condition_evaluation_retains_the_complete_accepted_maximum_trace() {
    fn equal(expected: &str) -> ResolvedPredicate {
        ResolvedPredicate::Equals([
            ResolvedOperand::text_reference("imports.prompt"),
            ResolvedOperand::text_literal(expected),
        ])
    }

    let text = CapturedText::new(Arc::from("yes"));
    let mut values = ConditionValues::default();
    values.insert_text("imports.prompt", &text);
    let mut children = Vec::new();
    for _ in 0..62 {
        children.push(ResolvedPredicate::All(
            vec![equal("yes"), equal("yes"), equal("yes")].into(),
        ));
    }
    children.push(ResolvedPredicate::All(
        vec![
            equal("yes"),
            equal("yes"),
            equal("yes"),
            equal("yes"),
            equal("yes"),
        ]
        .into(),
    ));
    children.push(equal("no"));
    let predicate = ResolvedPredicate::All(children.into());

    let ConditionEvaluation::False {
        evaluated_predicates,
    } = evaluate(&predicate, &values, &ConditionDispositions::default())
    else {
        panic!("the maximal accepted tree should complete false");
    };
    assert_eq!(evaluated_predicates.len(), 256);
    assert_eq!(
        evaluated_predicates.last(),
        Some(&EvaluatedPredicate {
            path: PredicatePath::root(),
            result: false,
        })
    );
}

#[test]
fn condition_evaluation_supports_every_terminal_disposition_without_source_detail() {
    for disposition in [
        TerminalDisposition::Succeeded,
        TerminalDisposition::Failed,
        TerminalDisposition::Skipped,
        TerminalDisposition::Blocked,
        TerminalDisposition::NotRun,
        TerminalDisposition::Cancelled,
    ] {
        let target = node("target");
        let mut dispositions = ConditionDispositions::default();
        dispositions.insert(target.clone(), disposition);
        let predicate = ResolvedPredicate::Disposition {
            node: target,
            is: disposition,
        };
        assert_eq!(
            evaluate(&predicate, &ConditionValues::default(), &dispositions),
            ConditionEvaluation::Passed
        );
    }
}

#[test]
fn condition_schema_accepts_the_active_grammar() {
    let document = decode(
        br#"schemaVersion: 1
steps:
  guarded:
    kind: cmd
    condition:
      equals:
        - { ref: imports.prompt }
        - { value: yes }
    command: { argv: ["true"] }
"#,
    )
    .unwrap();
    assert_eq!(document.steps.len(), 1);
}

#[test]
fn review_condition_trace_fits_source_bound_transition_capacity() {
    fn chain(expected: &str) -> (ResolvedPredicate, Value) {
        let mut predicate = ResolvedPredicate::Equals([
            ResolvedOperand::text_reference("imports.prompt"),
            ResolvedOperand::text_literal(expected),
        ]);
        let mut authored = json!({
            "equals": [
                { "ref": "imports.prompt" },
                { "value": expected }
            ]
        });
        for _ in 0..14 {
            predicate = ResolvedPredicate::Not(Box::new(predicate));
            authored = json!({ "not": authored });
        }
        (predicate, authored)
    }

    let mut predicates = Vec::new();
    let mut authored = Vec::new();
    for index in 0..17 {
        let (predicate, source) = chain(if index == 16 { "no" } else { "yes" });
        predicates.push(predicate);
        authored.push(source);
    }
    let source = serde_json::to_vec(&json!({
        "schemaVersion": 1,
        "steps": {
            "guarded": {
                "kind": "cmd",
                "condition": { "all": authored },
                "command": { "argv": ["x"] }
            }
        }
    }))
    .unwrap();

    let text = CapturedText::new(Arc::from("yes"));
    let mut values = ConditionValues::default();
    values.insert_text("imports.prompt", &text);
    let ConditionEvaluation::False {
        evaluated_predicates,
    } = evaluate(
        &ResolvedPredicate::All(predicates.into()),
        &values,
        &ConditionDispositions::default(),
    )
    else {
        panic!("the final child should make the root false");
    };
    assert_eq!(evaluated_predicates.len(), 256);

    let entries = evaluated_predicates
        .iter()
        .map(|entry| json!({ "path": entry.path.as_str(), "result": entry.result }))
        .collect::<Vec<_>>();
    let encoded_detail = serde_json::to_vec(&json!({
        "state": "skipped",
        "detail": {
            "code": "condition_false",
            "evaluatedPredicates": entries
        }
    }))
    .unwrap();
    let capacity = crate::execution::workflow::capacity::calculate_condition_evidence_capacity(
        u64::try_from(source.len()).unwrap(),
    )
    .unwrap();
    assert!(
        u64::try_from(encoded_detail.len()).unwrap() <= capacity.condition_transition_bytes,
        "{} detail bytes exceed the {}-byte bound derived from {} source bytes",
        encoded_detail.len(),
        capacity.condition_transition_bytes,
        source.len()
    );
}
