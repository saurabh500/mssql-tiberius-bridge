#[test]
fn missing_tiberius_data_apis_are_expected_compile_failures() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile_fail/data_api_*.rs");
    cases.pass("tests/pass/data_api_*.rs");
}
