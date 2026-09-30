#[test]
fn help_snapshots_match_rendered_output() {
    trycmd::TestCases::new().case("tests/cmd/help/*.trycmd");
}

#[test]
fn workflow_input_failure_diagnostic_snapshot() {
    trycmd::TestCases::new().case("tests/cmd/workflow-input-failure.trycmd");
}
