#[test]
fn missing_tiberius_data_apis_are_expected_compile_failures() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/pass/data_api_*.rs");
}
