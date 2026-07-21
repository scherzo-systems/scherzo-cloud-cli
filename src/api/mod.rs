#[allow(
    dead_code,
    unused_imports,
    clippy::derivable_impls,
    clippy::enum_variant_names,
    clippy::needless_return,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::uninlined_format_args
)]
mod generated;

#[cfg(test)]
mod tests {
    use super::generated;

    #[test]
    fn generated_problem_preserves_opaque_actions() {
        let input = serde_json::json!({
            "type": "https://api.scherzo.dev/problems/principal-not-provisioned",
            "title": "Principal not provisioned",
            "status": 403,
            "actions": [{
                "id": "future.action",
                "kind": "future-representation",
                "guide": "https://example.invalid/future-action",
                "additionalField": { "preserved": true }
            }]
        });

        let problem: generated::models::Problem =
            serde_json::from_value(input.clone()).expect("problem should decode");
        let actions = problem.actions.expect("actions should be present");

        assert_eq!(actions, input["actions"].as_array().unwrap().to_owned());
    }
}
