#[test]
fn help_snapshots_match_rendered_output() {
    trycmd::TestCases::new().case("tests/cmd/help/*.trycmd");
}
