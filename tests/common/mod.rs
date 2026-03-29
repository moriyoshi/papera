use papera::dialect::SourceDialect;

/// Assert that source SQL transpiles to the expected DuckDB SQL.
#[allow(dead_code)]
pub fn assert_transpiles(source_sql: &str, dialect: SourceDialect, expected_sql: &str) {
    let result = papera::transpile(source_sql, dialect).unwrap();
    pretty_assertions::assert_eq!(
        normalize_whitespace(&result),
        normalize_whitespace(expected_sql),
        "\n--- Source SQL ---\n{source_sql}\n--- Dialect: {dialect:?} ---"
    );
}

/// Assert that transpilation fails with an Unsupported error.
#[allow(dead_code)]
pub fn assert_unsupported(source_sql: &str, dialect: SourceDialect) {
    let result = papera::transpile(source_sql, dialect);
    assert!(
        matches!(result, Err(papera::Error::Unsupported(_))),
        "Expected Unsupported error, got: {result:?}"
    );
}

fn normalize_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
