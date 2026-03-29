use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, CaseWhen, DataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, UnaryOperator,
    Value, ValueWithSpan, helpers::attached_token::AttachedToken,
};

use crate::Result;
use crate::dialect::SourceDialect;
use crate::transforms::format_strings;

/// Describes how a function should be mapped from source dialect to DuckDB.
pub enum FunctionMapping {
    /// Simple rename: keep args unchanged, just change the function name.
    Rename(&'static str),
    /// Rename and reorder arguments. Each entry in the slice is the index of the
    /// original argument that should appear at that position.
    RenameReorder(&'static str, &'static [usize]),
    /// Custom transformation that can arbitrarily modify the Function AST node.
    /// Returns Ok(None) if the function was rewritten in place.
    /// Returns Ok(Some(expr)) if the function should be replaced by a different Expr.
    Custom(fn(&mut Function) -> Result<Option<Expr>>),
}

/// Build the function mapping table for the given dialect.
pub fn function_mappings(dialect: SourceDialect) -> HashMap<&'static str, FunctionMapping> {
    match dialect {
        SourceDialect::Trino => trino_mappings(),
        SourceDialect::Redshift => redshift_mappings(),
        // Hive functions are mostly a subset of Trino
        SourceDialect::Hive => trino_mappings(),
    }
}

// ---------------------------------------------------------------------------
// Trino → DuckDB
// ---------------------------------------------------------------------------

fn trino_mappings() -> HashMap<&'static str, FunctionMapping> {
    HashMap::from([
        (
            "approx_distinct",
            FunctionMapping::Rename("approx_count_distinct"),
        ),
        ("arbitrary", FunctionMapping::Rename("any_value")),
        (
            "json_extract_scalar",
            FunctionMapping::Rename("json_extract_string"),
        ),
        ("json_extract", FunctionMapping::Rename("json_extract")),
        ("from_unixtime", FunctionMapping::Rename("to_timestamp")),
        ("date_parse", FunctionMapping::Custom(trino_date_parse)),
        (
            "format_datetime",
            FunctionMapping::Custom(trino_format_datetime),
        ),
        ("transform", FunctionMapping::Rename("list_transform")),
        ("sequence", FunctionMapping::Rename("generate_series")),
        ("element_at", FunctionMapping::Rename("list_extract")),
        ("cardinality", FunctionMapping::Rename("len")),
        ("typeof", FunctionMapping::Rename("typeof")),
        ("chr", FunctionMapping::Rename("chr")),
        ("codepoint", FunctionMapping::Rename("unicode")),
        ("to_utf8", FunctionMapping::Custom(trino_to_utf8)),
        ("from_utf8", FunctionMapping::Custom(trino_from_utf8)),
        ("array_join", FunctionMapping::Rename("array_to_string")),
        ("reduce", FunctionMapping::Rename("list_reduce")),
        ("filter", FunctionMapping::Rename("list_filter")),
        ("contains", FunctionMapping::Rename("list_contains")),
        ("zip", FunctionMapping::Rename("list_zip")),
        ("flatten", FunctionMapping::Rename("flatten")),
        ("slice", FunctionMapping::Rename("list_slice")),
        ("array_distinct", FunctionMapping::Rename("list_distinct")),
        ("array_sort", FunctionMapping::Rename("list_sort")),
        ("array_max", FunctionMapping::Rename("list_max")),
        ("array_min", FunctionMapping::Rename("list_min")),
        ("array_position", FunctionMapping::Rename("list_position")),
        ("array_remove", FunctionMapping::Rename("list_filter")), // approximate
        ("map_keys", FunctionMapping::Rename("map_keys")),
        ("map_values", FunctionMapping::Rename("map_values")),
        ("regexp_like", FunctionMapping::Rename("regexp_matches")),
        ("regexp_extract", FunctionMapping::Rename("regexp_extract")),
        ("regexp_replace", FunctionMapping::Rename("regexp_replace")),
        ("strpos", FunctionMapping::Rename("strpos")),
        ("length", FunctionMapping::Rename("length")),
        ("reverse", FunctionMapping::Rename("reverse")),
        ("lpad", FunctionMapping::Rename("lpad")),
        ("rpad", FunctionMapping::Rename("rpad")),
        (
            "url_extract_host",
            FunctionMapping::Custom(trino_url_extract_host),
        ),
        (
            "url_extract_path",
            FunctionMapping::Custom(trino_url_extract_path),
        ),
        (
            "url_extract_protocol",
            FunctionMapping::Custom(trino_url_extract_protocol),
        ),
        (
            "url_extract_query",
            FunctionMapping::Custom(trino_url_extract_query),
        ),
        (
            "url_extract_fragment",
            FunctionMapping::Custom(trino_url_extract_fragment),
        ),
        (
            "url_extract_port",
            FunctionMapping::Custom(trino_url_extract_port),
        ),
        // Date/time
        ("date_diff", FunctionMapping::Rename("date_diff")),
        ("date_add", FunctionMapping::Rename("date_add")),
        ("day_of_week", FunctionMapping::Rename("dayofweek")),
        ("day_of_year", FunctionMapping::Rename("dayofyear")),
        ("week_of_year", FunctionMapping::Rename("weekofyear")),
        ("year_of_week", FunctionMapping::Rename("yearofweek")),
        // String
        ("split", FunctionMapping::Rename("str_split")),
        (
            "levenshtein_distance",
            FunctionMapping::Rename("levenshtein"),
        ),
        // Array
        ("array_intersect", FunctionMapping::Rename("list_intersect")),
        ("array_concat", FunctionMapping::Rename("list_concat")),
        ("array_except", FunctionMapping::Rename("list_except")),
        ("array_union", FunctionMapping::Custom(trino_array_union)),
        (
            "arrays_overlap",
            FunctionMapping::Custom(trino_arrays_overlap),
        ),
        ("array_sum", FunctionMapping::Rename("list_sum")),
        ("array_average", FunctionMapping::Rename("list_avg")),
        ("array_has", FunctionMapping::Rename("list_contains")),
        (
            "array_has_all",
            FunctionMapping::Custom(trino_array_has_all),
        ),
        (
            "array_has_any",
            FunctionMapping::Custom(trino_array_has_any),
        ),
        // JSON
        ("json_parse", FunctionMapping::Custom(trino_json_parse)),
        ("json_format", FunctionMapping::Custom(trino_json_format)),
        (
            "json_array_get",
            FunctionMapping::Custom(trino_json_array_get),
        ),
        ("json_object_keys", FunctionMapping::Rename("json_keys")),
        (
            "json_array_length",
            FunctionMapping::Rename("json_array_length"),
        ),
        // Aggregate
        (
            "approx_percentile",
            FunctionMapping::Rename("approx_quantile"),
        ),
        // Math
        ("is_nan", FunctionMapping::Rename("isnan")),
        ("is_finite", FunctionMapping::Rename("isfinite")),
        ("is_infinite", FunctionMapping::Rename("isinf")),
        ("nan", FunctionMapping::Custom(trino_nan)),
        ("infinity", FunctionMapping::Custom(trino_infinity)),
        // Bitwise
        ("bitwise_and", FunctionMapping::Custom(trino_bitwise_and)),
        ("bitwise_or", FunctionMapping::Custom(trino_bitwise_or)),
        ("bitwise_xor", FunctionMapping::Custom(trino_bitwise_xor)),
        ("bitwise_not", FunctionMapping::Custom(trino_bitwise_not)),
        (
            "bitwise_left_shift",
            FunctionMapping::Custom(trino_bitwise_left_shift),
        ),
        (
            "bitwise_right_shift",
            FunctionMapping::Custom(trino_bitwise_right_shift),
        ),
        // Additional mappings
        ("from_hex", FunctionMapping::Rename("unhex")),
        ("rand", FunctionMapping::Rename("random")),
        (
            "date_format",
            FunctionMapping::Custom(trino_format_datetime),
        ),
        ("at_timezone", FunctionMapping::Custom(trino_at_timezone)),
        ("to_unixtime", FunctionMapping::Rename("epoch")),
        ("parse_datetime", FunctionMapping::Custom(trino_date_parse)),
        ("with_timezone", FunctionMapping::Custom(trino_at_timezone)),
        (
            "current_timezone",
            FunctionMapping::Custom(trino_current_timezone),
        ),
        ("map_agg", FunctionMapping::Custom(trino_map_agg)),
    ])
}

fn trino_to_utf8(func: &mut Function) -> Result<Option<Expr>> {
    // to_utf8(s) → encode(s) in DuckDB (returns BLOB)
    set_function_name(func, "encode");
    Ok(None)
}

fn trino_from_utf8(func: &mut Function) -> Result<Option<Expr>> {
    // from_utf8(b) → decode(b) in DuckDB
    set_function_name(func, "decode");
    Ok(None)
}

/// date_parse(str, fmt) → strptime(str, converted_fmt)
/// Trino's date_parse accepts strftime-compatible format strings in many cases,
/// but also accepts Java-style patterns. Convert format string if it's a literal.
fn trino_date_parse(func: &mut Function) -> Result<Option<Expr>> {
    set_function_name(func, "strptime");
    convert_format_arg_trino(func, 1);
    Ok(None)
}

/// format_datetime(ts, fmt) → strftime(ts, converted_fmt)
/// Trino's format_datetime uses Java DateTimeFormatter patterns.
fn trino_format_datetime(func: &mut Function) -> Result<Option<Expr>> {
    set_function_name(func, "strftime");
    convert_format_arg_trino(func, 1);
    Ok(None)
}

/// at_timezone(ts, tz) → ts AT TIME ZONE tz
fn trino_at_timezone(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "at_timezone requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let timestamp = args.next().unwrap();
    let timezone = args.next().unwrap();
    Ok(Some(Expr::AtTimeZone {
        timestamp: Box::new(timestamp),
        time_zone: Box::new(timezone),
    }))
}

/// current_timezone() → current_setting('TimeZone')
fn trino_current_timezone(_func: &mut Function) -> Result<Option<Expr>> {
    let arg = Expr::Value(Value::SingleQuotedString("TimeZone".to_string()).into());
    Ok(Some(Expr::Function(make_function(
        "current_setting",
        vec![arg],
    ))))
}

/// map_agg(key, value) → map(list(key), list(value))
fn trino_map_agg(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "map_agg requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let key = args.next().unwrap();
    let value = args.next().unwrap();
    let list_keys = Expr::Function(make_function("list", vec![key]));
    let list_vals = Expr::Function(make_function("list", vec![value]));
    Ok(Some(Expr::Function(make_function(
        "map",
        vec![list_keys, list_vals],
    ))))
}

fn trino_url_extract_host(func: &mut Function) -> Result<Option<Expr>> {
    // url_extract_host(url) → DuckDB doesn't have a direct equivalent
    // Use regexp_extract as approximation
    // regexp_extract(url, '://([^/:]+)', 1)
    let args = extract_args(func);
    if let Some(url_arg) = args.into_iter().next() {
        let pattern = Expr::Value(Value::SingleQuotedString("://([^/:]+)".to_string()).into());
        let group = Expr::Value(Value::Number("1".to_string(), false).into());
        *func = make_function("regexp_extract", vec![url_arg, pattern, group]);
    }
    Ok(None)
}

fn trino_url_extract_path(func: &mut Function) -> Result<Option<Expr>> {
    // url_extract_path(url) → regexp_extract(url, '^[^?#]+')
    let args = extract_args(func);
    if let Some(url_arg) = args.into_iter().next() {
        let pattern = Expr::Value(Value::SingleQuotedString("^[^?#]+".to_string()).into());
        *func = make_function("regexp_extract", vec![url_arg, pattern]);
    }
    Ok(None)
}

fn trino_url_extract_protocol(func: &mut Function) -> Result<Option<Expr>> {
    // url_extract_protocol(url) → regexp_extract(url, '^([a-zA-Z][a-zA-Z0-9+\-.]*):\/\/', 1)
    let args = extract_args(func);
    if let Some(url_arg) = args.into_iter().next() {
        let pattern = Expr::Value(
            Value::SingleQuotedString("^([a-zA-Z][a-zA-Z0-9+\\-.]*)://".to_string()).into(),
        );
        let group = Expr::Value(Value::Number("1".to_string(), false).into());
        *func = make_function("regexp_extract", vec![url_arg, pattern, group]);
    }
    Ok(None)
}

fn trino_url_extract_query(func: &mut Function) -> Result<Option<Expr>> {
    // url_extract_query(url) → regexp_extract(url, '[?]([^#]*)', 1)
    let args = extract_args(func);
    if let Some(url_arg) = args.into_iter().next() {
        let pattern = Expr::Value(Value::SingleQuotedString("[?]([^#]*)".to_string()).into());
        let group = Expr::Value(Value::Number("1".to_string(), false).into());
        *func = make_function("regexp_extract", vec![url_arg, pattern, group]);
    }
    Ok(None)
}

fn trino_url_extract_fragment(func: &mut Function) -> Result<Option<Expr>> {
    // url_extract_fragment(url) → regexp_extract(url, '#(.*)', 1)
    let args = extract_args(func);
    if let Some(url_arg) = args.into_iter().next() {
        let pattern = Expr::Value(Value::SingleQuotedString("#(.*)".to_string()).into());
        let group = Expr::Value(Value::Number("1".to_string(), false).into());
        *func = make_function("regexp_extract", vec![url_arg, pattern, group]);
    }
    Ok(None)
}

fn trino_url_extract_port(func: &mut Function) -> Result<Option<Expr>> {
    // url_extract_port(url) → regexp_extract(url, '://[^/?#]*:([0-9]+)', 1)
    let args = extract_args(func);
    if let Some(url_arg) = args.into_iter().next() {
        let pattern =
            Expr::Value(Value::SingleQuotedString("://[^/?#]*:([0-9]+)".to_string()).into());
        let group = Expr::Value(Value::Number("1".to_string(), false).into());
        *func = make_function("regexp_extract", vec![url_arg, pattern, group]);
    }
    Ok(None)
}

fn trino_array_union(func: &mut Function) -> Result<Option<Expr>> {
    // array_union(a, b) → list_distinct(list_concat(a, b))
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "array_union requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    let concat = Expr::Function(make_function("list_concat", vec![a, b]));
    Ok(Some(Expr::Function(make_function(
        "list_distinct",
        vec![concat],
    ))))
}

fn trino_arrays_overlap(func: &mut Function) -> Result<Option<Expr>> {
    // arrays_overlap(a, b) → len(list_intersect(a, b)) > 0
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "arrays_overlap requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    let intersect = Expr::Function(make_function("list_intersect", vec![a, b]));
    let len = Expr::Function(make_function("len", vec![intersect]));
    Ok(Some(Expr::BinaryOp {
        left: Box::new(len),
        op: BinaryOperator::Gt,
        right: Box::new(Expr::Value(Value::Number("0".to_string(), false).into())),
    }))
}

fn trino_nan(_func: &mut Function) -> Result<Option<Expr>> {
    // nan() → CAST('NaN' AS DOUBLE)
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(Expr::Value(
            Value::SingleQuotedString("NaN".to_string()).into(),
        )),
        data_type: DataType::Double(sqlparser::ast::ExactNumberInfo::None),
        format: None,
        array: false,
    }))
}

fn trino_infinity(_func: &mut Function) -> Result<Option<Expr>> {
    // infinity() → CAST('Infinity' AS DOUBLE)
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(Expr::Value(
            Value::SingleQuotedString("Infinity".to_string()).into(),
        )),
        data_type: DataType::Double(sqlparser::ast::ExactNumberInfo::None),
        format: None,
        array: false,
    }))
}

fn trino_bitwise_and(func: &mut Function) -> Result<Option<Expr>> {
    // bitwise_and(a, b) → a & b
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "bitwise_and requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    Ok(Some(Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::BitwiseAnd,
        right: Box::new(b),
    }))
}

fn trino_bitwise_or(func: &mut Function) -> Result<Option<Expr>> {
    // bitwise_or(a, b) → a | b
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "bitwise_or requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    Ok(Some(Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::BitwiseOr,
        right: Box::new(b),
    }))
}

fn trino_bitwise_xor(func: &mut Function) -> Result<Option<Expr>> {
    // bitwise_xor(a, b) → a ^ b
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "bitwise_xor requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    Ok(Some(Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::BitwiseXor,
        right: Box::new(b),
    }))
}

fn trino_bitwise_not(func: &mut Function) -> Result<Option<Expr>> {
    // bitwise_not(a) → ~a
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "bitwise_not requires exactly 1 argument".to_string(),
        ));
    }
    let expr = args.into_iter().next().unwrap();
    Ok(Some(Expr::UnaryOp {
        op: UnaryOperator::BitwiseNot,
        expr: Box::new(expr),
    }))
}

fn trino_bitwise_left_shift(func: &mut Function) -> Result<Option<Expr>> {
    // bitwise_left_shift(a, b) → a << b
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "bitwise_left_shift requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    Ok(Some(Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::PGBitwiseShiftLeft,
        right: Box::new(b),
    }))
}

fn trino_bitwise_right_shift(func: &mut Function) -> Result<Option<Expr>> {
    // bitwise_right_shift(a, b) → a >> b
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "bitwise_right_shift requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let a = args.next().unwrap();
    let b = args.next().unwrap();
    Ok(Some(Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOperator::PGBitwiseShiftRight,
        right: Box::new(b),
    }))
}

// ---------------------------------------------------------------------------
// Redshift → DuckDB
// ---------------------------------------------------------------------------

fn redshift_mappings() -> HashMap<&'static str, FunctionMapping> {
    HashMap::from([
        ("getdate", FunctionMapping::Rename("current_timestamp")),
        ("sysdate", FunctionMapping::Rename("current_timestamp")),
        ("nvl", FunctionMapping::Rename("coalesce")),
        ("nvl2", FunctionMapping::Custom(redshift_nvl2)),
        ("decode", FunctionMapping::Custom(redshift_decode)),
        ("listagg", FunctionMapping::Rename("string_agg")),
        ("strtol", FunctionMapping::Custom(redshift_strtol)),
        (
            "convert_timezone",
            FunctionMapping::Custom(redshift_convert_timezone),
        ),
        ("regexp_substr", FunctionMapping::Rename("regexp_extract")),
        (
            "regexp_count",
            FunctionMapping::Custom(redshift_regexp_count),
        ),
        ("len", FunctionMapping::Rename("length")),
        ("charindex", FunctionMapping::Custom(redshift_charindex)),
        ("btrim", FunctionMapping::Rename("trim")),
        (
            "json_extract_path_text",
            FunctionMapping::Custom(redshift_json_extract_path_text),
        ),
        (
            "json_extract_array_element_text",
            FunctionMapping::Custom(redshift_json_extract_array_element),
        ),
        ("bpcharcmp", FunctionMapping::Custom(redshift_unsupported)),
        ("dateadd", FunctionMapping::Custom(redshift_dateadd)),
        ("datediff", FunctionMapping::Custom(redshift_datediff)),
        (
            "date_trunc",
            FunctionMapping::Custom(redshift_quote_first_arg),
        ),
        (
            "date_part",
            FunctionMapping::Custom(redshift_quote_first_arg),
        ),
        ("to_char", FunctionMapping::Custom(redshift_to_char)),
        ("to_date", FunctionMapping::Custom(redshift_to_date)),
        (
            "to_timestamp",
            FunctionMapping::Custom(redshift_to_timestamp),
        ),
        ("trim", FunctionMapping::Rename("trim")),
        ("replace", FunctionMapping::Rename("replace")),
        ("upper", FunctionMapping::Rename("upper")),
        ("lower", FunctionMapping::Rename("lower")),
        ("left", FunctionMapping::Rename("left")),
        ("right", FunctionMapping::Rename("right")),
        ("substring", FunctionMapping::Rename("substring")),
        ("md5", FunctionMapping::Rename("md5")),
        ("sha1", FunctionMapping::Rename("sha1")),
        ("lcase", FunctionMapping::Rename("lower")),
        ("ucase", FunctionMapping::Rename("upper")),
        ("is_valid_json", FunctionMapping::Rename("json_valid")),
        ("isnull", FunctionMapping::Custom(redshift_isnull)),
        ("space", FunctionMapping::Custom(redshift_space)),
        ("sha2", FunctionMapping::Custom(redshift_sha2)),
        // JSON
        ("json_typeof", FunctionMapping::Rename("json_type")),
        (
            "json_serialize",
            FunctionMapping::Custom(redshift_json_serialize),
        ),
        (
            "json_deserialize",
            FunctionMapping::Custom(redshift_json_deserialize),
        ),
        (
            "json_array_length",
            FunctionMapping::Rename("json_array_length"),
        ),
        // Array
        ("array_concat", FunctionMapping::Rename("list_concat")),
        // Additional mappings
        (
            "months_between",
            FunctionMapping::Custom(redshift_months_between),
        ),
        ("add_months", FunctionMapping::Custom(redshift_add_months)),
        (
            "ratio_to_report",
            FunctionMapping::Custom(redshift_ratio_to_report),
        ),
    ])
}

/// NVL2(expr, val_if_not_null, val_if_null) → CASE WHEN expr IS NOT NULL THEN ... ELSE ... END
fn redshift_nvl2(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 3 {
        return Err(crate::Error::Unsupported(
            "NVL2 requires exactly 3 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let expr = args.next().unwrap();
    let val_not_null = args.next().unwrap();
    let val_null = args.next().unwrap();

    Ok(Some(Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: Expr::IsNotNull(Box::new(expr)),
            result: val_not_null,
        }],
        else_result: Some(Box::new(val_null)),
    }))
}

/// DECODE(expr, search1, result1, search2, result2, ..., default)
/// → CASE expr WHEN search1 THEN result1 WHEN search2 THEN result2 ... ELSE default END
fn redshift_decode(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() < 3 {
        return Err(crate::Error::Unsupported(
            "DECODE requires at least 3 arguments".to_string(),
        ));
    }

    let mut args = args.into_iter();
    let operand = args.next().unwrap();
    let remaining: Vec<Expr> = args.collect();

    let mut conditions = Vec::new();
    let mut else_result = None;

    let mut i = 0;
    while i < remaining.len() {
        if i + 1 < remaining.len() {
            conditions.push(CaseWhen {
                condition: remaining[i].clone(),
                result: remaining[i + 1].clone(),
            });
            i += 2;
        } else {
            // Odd trailing arg is the default
            else_result = Some(Box::new(remaining[i].clone()));
            i += 1;
        }
    }

    Ok(Some(Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: Some(Box::new(operand)),
        conditions,
        else_result,
    }))
}

/// STRTOL(str, base) →
///   CASE base
///     WHEN 16 THEN CAST(('0x' || str) AS BIGINT)
///     WHEN 10 THEN CAST(str AS BIGINT)
///   END
///
/// Uses CASE WHEN so the base can be a runtime expression.
fn redshift_strtol(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "STRTOL requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let str_arg = args.next().unwrap();
    let base_arg = args.next().unwrap();

    // WHEN 16 THEN CAST(('0x' || str) AS BIGINT)
    let hex_prefixed = Expr::BinaryOp {
        left: Box::new(Expr::Value(
            Value::SingleQuotedString("0x".to_string()).into(),
        )),
        op: BinaryOperator::StringConcat,
        right: Box::new(str_arg.clone()),
    };
    let hex_cast = Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(Expr::Nested(Box::new(hex_prefixed))),
        data_type: DataType::BigInt(None),
        format: None,
        array: false,
    };

    // WHEN 10 THEN CAST(str AS BIGINT)
    let dec_cast = Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(str_arg),
        data_type: DataType::BigInt(None),
        format: None,
        array: false,
    };

    Ok(Some(Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: Some(Box::new(base_arg)),
        conditions: vec![
            CaseWhen {
                condition: Expr::Value(Value::Number("16".to_string(), false).into()),
                result: hex_cast,
            },
            CaseWhen {
                condition: Expr::Value(Value::Number("10".to_string(), false).into()),
                result: dec_cast,
            },
        ],
        else_result: None,
    }))
}

/// CONVERT_TIMEZONE('source_tz', 'target_tz', timestamp) or
/// CONVERT_TIMEZONE('target_tz', timestamp)
fn redshift_convert_timezone(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    match args.len() {
        2 => {
            // CONVERT_TIMEZONE('target_tz', timestamp) → timestamp AT TIME ZONE 'target_tz'
            let mut args = args.into_iter();
            let target_tz = args.next().unwrap();
            let timestamp = args.next().unwrap();
            Ok(Some(Expr::AtTimeZone {
                timestamp: Box::new(timestamp),
                time_zone: Box::new(target_tz),
            }))
        }
        3 => {
            // CONVERT_TIMEZONE('source', 'target', ts) →
            // (ts AT TIME ZONE 'source') AT TIME ZONE 'target'
            let mut args = args.into_iter();
            let source_tz = args.next().unwrap();
            let target_tz = args.next().unwrap();
            let timestamp = args.next().unwrap();
            let at_source = Expr::AtTimeZone {
                timestamp: Box::new(timestamp),
                time_zone: Box::new(source_tz),
            };
            Ok(Some(Expr::AtTimeZone {
                timestamp: Box::new(at_source),
                time_zone: Box::new(target_tz),
            }))
        }
        _ => Err(crate::Error::Unsupported(
            "CONVERT_TIMEZONE requires 2 or 3 arguments".to_string(),
        )),
    }
}

/// REGEXP_COUNT(str, pattern) → length(regexp_extract_all(str, pattern))
fn redshift_regexp_count(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() < 2 {
        return Err(crate::Error::Unsupported(
            "REGEXP_COUNT requires at least 2 arguments".to_string(),
        ));
    }
    let mut args_iter = args.into_iter();
    let str_arg = args_iter.next().unwrap();
    let pattern = args_iter.next().unwrap();

    let extract_all = Expr::Function(make_function("regexp_extract_all", vec![str_arg, pattern]));
    let len_func = make_function("len", vec![extract_all]);
    Ok(Some(Expr::Function(len_func)))
}

/// CHARINDEX(substr, str) → strpos(str, substr)  (args swapped)
fn redshift_charindex(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() < 2 {
        return Err(crate::Error::Unsupported(
            "CHARINDEX requires at least 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let substr = args.next().unwrap();
    let str_arg = args.next().unwrap();

    *func = make_function("strpos", vec![str_arg, substr]);
    Ok(None)
}

/// JSON_EXTRACT_PATH_TEXT(json, 'key1', 'key2', ...)
/// → json_extract_string(json, '$.key1.key2...')
fn redshift_json_extract_path_text(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() < 2 {
        return Err(crate::Error::Unsupported(
            "JSON_EXTRACT_PATH_TEXT requires at least 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let json_arg = args.next().unwrap();

    let mut path = String::from("$");
    for key in args {
        if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) = &key
        {
            path.push('.');
            path.push_str(s);
        } else {
            // Dynamic path — fallback to chained access
            return Err(crate::Error::Unsupported(
                "JSON_EXTRACT_PATH_TEXT with non-literal keys".to_string(),
            ));
        }
    }

    let path_expr = Expr::Value(Value::SingleQuotedString(path).into());
    *func = make_function("json_extract_string", vec![json_arg, path_expr]);
    Ok(None)
}

/// JSON_EXTRACT_ARRAY_ELEMENT_TEXT(json, index)
/// → json_extract_string(json, '$[' || index || ']')
fn redshift_json_extract_array_element(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "JSON_EXTRACT_ARRAY_ELEMENT_TEXT requires 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let json_arg = args.next().unwrap();
    let index = args.next().unwrap();

    if let Expr::Value(ValueWithSpan {
        value: Value::Number(n, _),
        ..
    }) = &index
    {
        let path = format!("$[{n}]");
        let path_expr = Expr::Value(Value::SingleQuotedString(path).into());
        *func = make_function("json_extract_string", vec![json_arg, path_expr]);
        Ok(None)
    } else {
        Err(crate::Error::Unsupported(
            "JSON_EXTRACT_ARRAY_ELEMENT_TEXT with non-literal index".to_string(),
        ))
    }
}

fn redshift_unsupported(func: &mut Function) -> Result<Option<Expr>> {
    Err(crate::Error::Unsupported(format!(
        "Redshift function {} has no DuckDB equivalent",
        func.name,
    )))
}

/// array_has_all(arr, required) → len(list_intersect(arr, required)) = len(required)
fn trino_array_has_all(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "array_has_all requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let arr = args.next().unwrap();
    let required = args.next().unwrap();
    let intersect = Expr::Function(make_function("list_intersect", vec![arr, required.clone()]));
    let len_intersect = Expr::Function(make_function("len", vec![intersect]));
    let len_required = Expr::Function(make_function("len", vec![required]));
    Ok(Some(Expr::BinaryOp {
        left: Box::new(len_intersect),
        op: BinaryOperator::Eq,
        right: Box::new(len_required),
    }))
}

/// array_has_any(arr, candidates) → len(list_intersect(arr, candidates)) > 0
fn trino_array_has_any(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "array_has_any requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let arr = args.next().unwrap();
    let candidates = args.next().unwrap();
    let intersect = Expr::Function(make_function("list_intersect", vec![arr, candidates]));
    let len = Expr::Function(make_function("len", vec![intersect]));
    Ok(Some(Expr::BinaryOp {
        left: Box::new(len),
        op: BinaryOperator::Gt,
        right: Box::new(Expr::Value(Value::Number("0".to_string(), false).into())),
    }))
}

/// json_parse(str) → CAST(str AS JSON)
fn trino_json_parse(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "json_parse requires exactly 1 argument".to_string(),
        ));
    }
    let str_arg = args.into_iter().next().unwrap();
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(str_arg),
        data_type: DataType::JSON,
        format: None,
        array: false,
    }))
}

/// json_format(json) → CAST(json AS VARCHAR)
fn trino_json_format(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "json_format requires exactly 1 argument".to_string(),
        ));
    }
    let json_arg = args.into_iter().next().unwrap();
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(json_arg),
        data_type: DataType::Varchar(None),
        format: None,
        array: false,
    }))
}

/// json_array_get(json, idx) → json_extract_string(json, '$[idx]')  (literal index only)
fn trino_json_array_get(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "json_array_get requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let json_arg = args.next().unwrap();
    let idx = args.next().unwrap();

    if let Expr::Value(ValueWithSpan {
        value: Value::Number(ref n, _),
        ..
    }) = idx
    {
        let path = format!("$[{n}]");
        let path_expr = Expr::Value(Value::SingleQuotedString(path).into());
        *func = make_function("json_extract_string", vec![json_arg, path_expr]);
        Ok(None)
    } else {
        Err(crate::Error::Unsupported(
            "json_array_get with non-literal index is not supported".to_string(),
        ))
    }
}

/// ISNULL(val, replacement) → COALESCE(val, replacement)
/// Redshift 2-arg ISNULL is equivalent to NVL / COALESCE.
fn redshift_isnull(func: &mut Function) -> Result<Option<Expr>> {
    set_function_name(func, "coalesce");
    Ok(None)
}

/// SPACE(n) → REPEAT(' ', n)
fn redshift_space(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "SPACE requires exactly 1 argument".to_string(),
        ));
    }
    let n = args.into_iter().next().unwrap();
    let space_char = Expr::Value(Value::SingleQuotedString(" ".to_string()).into());
    *func = make_function("repeat", vec![space_char, n]);
    Ok(None)
}

/// SHA2(str, bits) → sha256(str)  (only 256-bit variant supported)
fn redshift_sha2(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "SHA2 requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let str_arg = args.next().unwrap();
    let bits_arg = args.next().unwrap();

    if let Expr::Value(ValueWithSpan {
        value: Value::Number(ref n, _),
        ..
    }) = bits_arg
        && n == "256"
    {
        *func = make_function("sha256", vec![str_arg]);
        return Ok(None);
    }
    Err(crate::Error::Unsupported(
        "SHA2 is only supported with bit length 256 for DuckDB translation".to_string(),
    ))
}

/// JSON_SERIALIZE(json) → CAST(json AS VARCHAR)
fn redshift_json_serialize(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "JSON_SERIALIZE requires exactly 1 argument".to_string(),
        ));
    }
    let json_arg = args.into_iter().next().unwrap();
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(json_arg),
        data_type: DataType::Varchar(None),
        format: None,
        array: false,
    }))
}

/// JSON_DESERIALIZE(str) → CAST(str AS JSON)
fn redshift_json_deserialize(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "JSON_DESERIALIZE requires exactly 1 argument".to_string(),
        ));
    }
    let str_arg = args.into_iter().next().unwrap();
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(str_arg),
        data_type: DataType::JSON,
        format: None,
        array: false,
    }))
}

/// TO_CHAR(ts, format) → strftime(ts, converted_format)
fn redshift_to_char(func: &mut Function) -> Result<Option<Expr>> {
    set_function_name(func, "strftime");
    convert_format_arg_redshift(func, 1);
    Ok(None)
}

/// TO_DATE(str, format) → CAST(strptime(str, converted_format) AS DATE)
fn redshift_to_date(func: &mut Function) -> Result<Option<Expr>> {
    set_function_name(func, "strptime");
    convert_format_arg_redshift(func, 1);
    // Wrap in CAST to DATE
    let func_expr = Expr::Function(func.clone());
    Ok(Some(Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(func_expr),
        data_type: sqlparser::ast::DataType::Date,
        format: None,
        array: false,
    }))
}

/// TO_TIMESTAMP(str, format) → strptime(str, converted_format)
fn redshift_to_timestamp(func: &mut Function) -> Result<Option<Expr>> {
    set_function_name(func, "strptime");
    convert_format_arg_redshift(func, 1);
    Ok(None)
}

/// DATEADD(datepart, interval, date) → date + INTERVAL 'interval' datepart
///
/// Redshift's 3-arg DATEADD uses an unquoted datepart keyword and integer interval.
/// We rewrite to DuckDB interval arithmetic for correctness.
fn redshift_dateadd(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 3 {
        return Err(crate::Error::Unsupported(
            "DATEADD requires exactly 3 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let datepart = args.next().unwrap();
    let interval_val = args.next().unwrap();
    let date_expr = args.next().unwrap();

    let part_str = expr_to_datepart_string(&datepart)?;
    let interval_expr = make_interval_expr(&interval_val, &part_str);

    Ok(Some(Expr::BinaryOp {
        left: Box::new(date_expr),
        op: BinaryOperator::Plus,
        right: Box::new(interval_expr),
    }))
}

/// DATEDIFF(datepart, start, end) → date_diff('datepart', start, end)
///
/// Redshift passes datepart as an unquoted keyword. DuckDB's date_diff expects
/// a quoted string literal as the first argument.
fn redshift_datediff(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 3 {
        return Err(crate::Error::Unsupported(
            "DATEDIFF requires exactly 3 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let datepart = args.next().unwrap();
    let start = args.next().unwrap();
    let end = args.next().unwrap();

    let part_str = expr_to_datepart_string(&datepart)?;
    let part_literal = Expr::Value(Value::SingleQuotedString(part_str).into());

    *func = make_function("date_diff", vec![part_literal, start, end]);
    Ok(None)
}

/// Extract a datepart string from an expression.
/// Redshift dateparts are typically parsed as identifiers (e.g., `month`, `day`)
/// or sometimes as string literals.
fn expr_to_datepart_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.to_lowercase()),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) => Ok(s.to_lowercase()),
        Expr::CompoundIdentifier(parts) => {
            // e.g., just use the last part
            if let Some(last) = parts.last() {
                Ok(last.value.to_lowercase())
            } else {
                Err(crate::Error::Unsupported(
                    "Cannot extract datepart from empty compound identifier".to_string(),
                ))
            }
        }
        _ => Err(crate::Error::Unsupported(format!(
            "Cannot extract datepart from expression: {expr}"
        ))),
    }
}

/// Build an INTERVAL expression: INTERVAL 'value' datepart
fn make_interval_expr(value: &Expr, datepart: &str) -> Expr {
    // We need to build INTERVAL 'N' DATEPART
    // For numeric literals, extract the value directly
    let interval_str = match value {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => n.clone(),
        _ => value.to_string(),
    };

    // Build: INTERVAL 'N' datepart
    // sqlparser represents this as Expr::Interval
    Expr::Interval(sqlparser::ast::Interval {
        value: Box::new(Expr::Value(Value::SingleQuotedString(interval_str).into())),
        leading_field: Some(str_to_date_time_field(datepart)),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    })
}

fn str_to_date_time_field(s: &str) -> sqlparser::ast::DateTimeField {
    use sqlparser::ast::DateTimeField;
    match s {
        "year" | "years" | "y" | "yr" | "yrs" => DateTimeField::Year,
        "quarter" | "quarters" | "qtr" | "qtrs" => DateTimeField::Quarter,
        "month" | "months" | "mon" | "mons" => DateTimeField::Month,
        "week" | "weeks" | "w" => DateTimeField::Week(None),
        "day" | "days" | "d" | "dayofyear" => DateTimeField::Day,
        "hour" | "hours" | "h" | "hr" | "hrs" => DateTimeField::Hour,
        "minute" | "minutes" | "m" | "min" | "mins" => DateTimeField::Minute,
        "second" | "seconds" | "s" | "sec" | "secs" => DateTimeField::Second,
        "millisecond" | "milliseconds" | "ms" => DateTimeField::Millisecond,
        "microsecond" | "microseconds" | "us" => DateTimeField::Microsecond,
        other => DateTimeField::Custom(Ident::new(other)),
    }
}

/// MONTHS_BETWEEN(date1, date2) → datediff('month', date2, date1)
fn redshift_months_between(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "MONTHS_BETWEEN requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let date1 = args.next().unwrap();
    let date2 = args.next().unwrap();
    let part = Expr::Value(Value::SingleQuotedString("month".to_string()).into());
    *func = make_function("datediff", vec![part, date2, date1]);
    Ok(None)
}

/// ADD_MONTHS(date, n) → date + INTERVAL 'n' MONTH
fn redshift_add_months(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 2 {
        return Err(crate::Error::Unsupported(
            "ADD_MONTHS requires exactly 2 arguments".to_string(),
        ));
    }
    let mut args = args.into_iter();
    let date_expr = args.next().unwrap();
    let n = args.next().unwrap();
    let interval = make_interval_expr(&n, "month");
    Ok(Some(Expr::BinaryOp {
        left: Box::new(date_expr),
        op: BinaryOperator::Plus,
        right: Box::new(interval),
    }))
}

/// RATIO_TO_REPORT(col) OVER (window) → col / SUM(col) OVER (window)
///
/// The OVER clause is attached to the Function AST node, so we build
/// a SUM(col) with the same OVER and return col / SUM(col) OVER (...).
fn redshift_ratio_to_report(func: &mut Function) -> Result<Option<Expr>> {
    let args = extract_args(func);
    if args.len() != 1 {
        return Err(crate::Error::Unsupported(
            "RATIO_TO_REPORT requires exactly 1 argument".to_string(),
        ));
    }
    let col = args.into_iter().next().unwrap();

    let mut sum_func = make_function("SUM", vec![col.clone()]);
    sum_func.over = func.over.clone();

    Ok(Some(Expr::BinaryOp {
        left: Box::new(col),
        op: BinaryOperator::Divide,
        right: Box::new(Expr::Function(sum_func)),
    }))
}

/// Quote the first argument if it's an unquoted identifier.
///
/// Redshift functions like DATE_PART(year, date) and DATE_TRUNC(month, ts)
/// accept unquoted keyword dateparts. DuckDB expects string literals.
/// If the first arg is already a string literal, leave it unchanged.
fn redshift_quote_first_arg(func: &mut Function) -> Result<Option<Expr>> {
    if let FunctionArguments::List(ref mut arg_list) = func.args
        && let Some(arg) = arg_list.args.first_mut()
        && let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
    {
        match expr {
            // Already a string literal — leave as-is
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(_),
                ..
            }) => {}
            // Identifier like `year`, `month` → convert to 'year', 'month'
            Expr::Identifier(ident) => {
                *expr = Expr::Value(Value::SingleQuotedString(ident.value.to_lowercase()).into());
            }
            _ => {}
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Replace the last identifier in a function name.
fn set_function_name(func: &mut Function, new_name: &str) {
    if let Some(last) = func.name.0.last_mut() {
        *last = ObjectNamePart::Identifier(Ident::new(new_name));
    }
}

/// Extract positional argument expressions from a Function.
fn extract_args(func: &Function) -> Vec<Expr> {
    match &func.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Some(e.clone()),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// Construct a simple function call expression.
fn make_function(name: &str, args: Vec<Expr>) -> Function {
    Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

/// If the function argument at `index` is a string literal, convert it from
/// Trino (Java DateTimeFormatter) format to DuckDB strftime format in place.
fn convert_format_arg_trino(func: &mut Function, index: usize) {
    convert_format_arg(func, index, format_strings::trino_to_strftime);
}

/// If the function argument at `index` is a string literal, convert it from
/// Redshift (PostgreSQL) format to DuckDB strftime format in place.
fn convert_format_arg_redshift(func: &mut Function, index: usize) {
    convert_format_arg(func, index, format_strings::redshift_to_strftime);
}

/// Generic helper: apply a conversion function to a string-literal argument.
fn convert_format_arg(func: &mut Function, index: usize, convert: fn(&str) -> String) {
    if let FunctionArguments::List(ref mut arg_list) = func.args
        && let Some(arg) = arg_list.args.get_mut(index)
        && let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }))) = arg
    {
        // Only convert if the format looks like it needs conversion
        // (contains alphabetic chars that aren't already %-prefixed)
        if !s.contains('%') {
            *s = convert(s);
        }
    }
}

/// Apply a function mapping to a Function AST node.
/// Returns Ok(None) if the function was rewritten in place.
/// Returns Ok(Some(expr)) if the function should be replaced by a different Expr.
pub fn apply_mapping(func: &mut Function, mapping: &FunctionMapping) -> Result<Option<Expr>> {
    match mapping {
        FunctionMapping::Rename(new_name) => {
            set_function_name(func, new_name);
            Ok(None)
        }
        FunctionMapping::RenameReorder(new_name, order) => {
            set_function_name(func, new_name);
            if let FunctionArguments::List(ref mut arg_list) = func.args {
                let old_args = arg_list.args.clone();
                for (i, &src_idx) in order.iter().enumerate() {
                    if src_idx < old_args.len() && i < arg_list.args.len() {
                        arg_list.args[i] = old_args[src_idx].clone();
                    }
                }
            }
            Ok(None)
        }
        FunctionMapping::Custom(f) => f(func),
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::SourceDialect;

    fn assert_transpiles(sql: &str, dialect: SourceDialect, expected: &str) {
        let result = crate::transpile(sql, dialect).unwrap();
        let normalize = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(normalize(&result), normalize(expected), "\nSQL: {sql}");
    }

    // Trino function tests
    #[test]
    fn trino_approx_distinct() {
        assert_transpiles(
            "SELECT approx_distinct(col) FROM t",
            SourceDialect::Trino,
            "SELECT approx_count_distinct(col) FROM t",
        );
    }

    #[test]
    fn trino_arbitrary() {
        assert_transpiles(
            "SELECT arbitrary(col) FROM t GROUP BY x",
            SourceDialect::Trino,
            "SELECT any_value(col) FROM t GROUP BY x",
        );
    }

    #[test]
    fn trino_json_extract_scalar() {
        assert_transpiles(
            "SELECT json_extract_scalar(data, '$.name') FROM t",
            SourceDialect::Trino,
            "SELECT json_extract_string(data, '$.name') FROM t",
        );
    }

    #[test]
    fn trino_date_parse() {
        assert_transpiles(
            "SELECT date_parse(s, '%Y-%m-%d') FROM t",
            SourceDialect::Trino,
            "SELECT strptime(s, '%Y-%m-%d') FROM t",
        );
    }

    // Redshift function tests
    #[test]
    fn redshift_nvl() {
        assert_transpiles(
            "SELECT NVL(a, b) FROM t",
            SourceDialect::Redshift,
            "SELECT coalesce(a, b) FROM t",
        );
    }

    #[test]
    fn redshift_nvl2() {
        assert_transpiles(
            "SELECT NVL2(a, 'yes', 'no') FROM t",
            SourceDialect::Redshift,
            "SELECT CASE WHEN a IS NOT NULL THEN 'yes' ELSE 'no' END FROM t",
        );
    }

    #[test]
    fn redshift_decode() {
        assert_transpiles(
            "SELECT DECODE(status, 1, 'active', 2, 'inactive', 'unknown') FROM t",
            SourceDialect::Redshift,
            "SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM t",
        );
    }

    #[test]
    fn redshift_getdate() {
        assert_transpiles(
            "SELECT getdate()",
            SourceDialect::Redshift,
            "SELECT current_timestamp()",
        );
    }

    #[test]
    fn redshift_listagg() {
        assert_transpiles(
            "SELECT listagg(name, ',') FROM t",
            SourceDialect::Redshift,
            "SELECT string_agg(name, ',') FROM t",
        );
    }

    #[test]
    fn redshift_charindex() {
        assert_transpiles(
            "SELECT CHARINDEX('world', greeting) FROM t",
            SourceDialect::Redshift,
            "SELECT strpos(greeting, 'world') FROM t",
        );
    }

    #[test]
    fn redshift_convert_timezone_2arg() {
        assert_transpiles(
            "SELECT CONVERT_TIMEZONE('US/Eastern', ts) FROM t",
            SourceDialect::Redshift,
            "SELECT ts AT TIME ZONE 'US/Eastern' FROM t",
        );
    }

    #[test]
    fn redshift_convert_timezone_3arg() {
        assert_transpiles(
            "SELECT CONVERT_TIMEZONE('UTC', 'US/Eastern', ts) FROM t",
            SourceDialect::Redshift,
            "SELECT ts AT TIME ZONE 'UTC' AT TIME ZONE 'US/Eastern' FROM t",
        );
    }

    #[test]
    fn redshift_json_extract_path_text() {
        assert_transpiles(
            "SELECT JSON_EXTRACT_PATH_TEXT(data, 'user', 'name') FROM t",
            SourceDialect::Redshift,
            "SELECT json_extract_string(data, '$.user.name') FROM t",
        );
    }

    // New Trino function tests
    #[test]
    fn trino_date_diff() {
        assert_transpiles(
            "SELECT date_diff('day', start_ts, end_ts) FROM t",
            SourceDialect::Trino,
            "SELECT date_diff('day', start_ts, end_ts) FROM t",
        );
    }

    #[test]
    fn trino_date_add() {
        assert_transpiles(
            "SELECT date_add('month', 3, created_at) FROM t",
            SourceDialect::Trino,
            "SELECT date_add('month', 3, created_at) FROM t",
        );
    }

    #[test]
    fn trino_day_of_week() {
        assert_transpiles(
            "SELECT day_of_week(d) FROM t",
            SourceDialect::Trino,
            "SELECT dayofweek(d) FROM t",
        );
    }

    #[test]
    fn trino_day_of_year() {
        assert_transpiles(
            "SELECT day_of_year(d) FROM t",
            SourceDialect::Trino,
            "SELECT dayofyear(d) FROM t",
        );
    }

    #[test]
    fn trino_week_of_year() {
        assert_transpiles(
            "SELECT week_of_year(d) FROM t",
            SourceDialect::Trino,
            "SELECT weekofyear(d) FROM t",
        );
    }

    #[test]
    fn trino_split() {
        assert_transpiles(
            "SELECT split(s, ',') FROM t",
            SourceDialect::Trino,
            "SELECT str_split(s, ',') FROM t",
        );
    }

    #[test]
    fn trino_levenshtein_distance() {
        assert_transpiles(
            "SELECT levenshtein_distance(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT levenshtein(a, b) FROM t",
        );
    }

    #[test]
    fn trino_array_intersect() {
        assert_transpiles(
            "SELECT array_intersect(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT list_intersect(a, b) FROM t",
        );
    }

    #[test]
    fn trino_array_concat() {
        assert_transpiles(
            "SELECT array_concat(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT list_concat(a, b) FROM t",
        );
    }

    #[test]
    fn trino_array_union() {
        assert_transpiles(
            "SELECT array_union(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT list_distinct(list_concat(a, b)) FROM t",
        );
    }

    #[test]
    fn trino_arrays_overlap() {
        assert_transpiles(
            "SELECT arrays_overlap(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT len(list_intersect(a, b)) > 0 FROM t",
        );
    }

    #[test]
    fn trino_approx_percentile() {
        assert_transpiles(
            "SELECT approx_percentile(score, 0.95) FROM t",
            SourceDialect::Trino,
            "SELECT approx_quantile(score, 0.95) FROM t",
        );
    }

    #[test]
    fn trino_is_nan() {
        assert_transpiles(
            "SELECT is_nan(x) FROM t",
            SourceDialect::Trino,
            "SELECT isnan(x) FROM t",
        );
    }

    #[test]
    fn trino_is_infinite() {
        assert_transpiles(
            "SELECT is_infinite(x) FROM t",
            SourceDialect::Trino,
            "SELECT isinf(x) FROM t",
        );
    }

    #[test]
    fn trino_nan_literal() {
        assert_transpiles(
            "SELECT nan() FROM t",
            SourceDialect::Trino,
            "SELECT CAST('NaN' AS DOUBLE) FROM t",
        );
    }

    #[test]
    fn trino_infinity_literal() {
        assert_transpiles(
            "SELECT infinity() FROM t",
            SourceDialect::Trino,
            "SELECT CAST('Infinity' AS DOUBLE) FROM t",
        );
    }

    #[test]
    fn trino_bitwise_and() {
        assert_transpiles(
            "SELECT bitwise_and(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT a & b FROM t",
        );
    }

    #[test]
    fn trino_bitwise_or() {
        assert_transpiles(
            "SELECT bitwise_or(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT a | b FROM t",
        );
    }

    #[test]
    fn trino_bitwise_xor() {
        assert_transpiles(
            "SELECT bitwise_xor(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT a ^ b FROM t",
        );
    }

    #[test]
    fn trino_bitwise_not() {
        assert_transpiles(
            "SELECT bitwise_not(a) FROM t",
            SourceDialect::Trino,
            "SELECT ~a FROM t",
        );
    }

    #[test]
    fn trino_bitwise_left_shift() {
        assert_transpiles(
            "SELECT bitwise_left_shift(a, 2) FROM t",
            SourceDialect::Trino,
            "SELECT a << 2 FROM t",
        );
    }

    #[test]
    fn trino_bitwise_right_shift() {
        assert_transpiles(
            "SELECT bitwise_right_shift(a, 2) FROM t",
            SourceDialect::Trino,
            "SELECT a >> 2 FROM t",
        );
    }

    #[test]
    fn trino_url_extract_query() {
        let result =
            crate::transpile("SELECT url_extract_query(url) FROM t", SourceDialect::Trino).unwrap();
        assert!(result.contains("regexp_extract"), "Got: {result}");
        assert!(result.contains("[?]([^#]*)"), "Got: {result}");
    }

    #[test]
    fn trino_url_extract_protocol() {
        let result = crate::transpile(
            "SELECT url_extract_protocol(url) FROM t",
            SourceDialect::Trino,
        )
        .unwrap();
        assert!(result.contains("regexp_extract"), "Got: {result}");
    }

    // New Redshift function tests
    #[test]
    fn redshift_lcase() {
        assert_transpiles(
            "SELECT lcase(name) FROM t",
            SourceDialect::Redshift,
            "SELECT lower(name) FROM t",
        );
    }

    #[test]
    fn redshift_ucase() {
        assert_transpiles(
            "SELECT ucase(name) FROM t",
            SourceDialect::Redshift,
            "SELECT upper(name) FROM t",
        );
    }

    #[test]
    fn redshift_isnull() {
        assert_transpiles(
            "SELECT ISNULL(col, 0) FROM t",
            SourceDialect::Redshift,
            "SELECT coalesce(col, 0) FROM t",
        );
    }

    #[test]
    fn redshift_space() {
        assert_transpiles(
            "SELECT SPACE(5) FROM t",
            SourceDialect::Redshift,
            "SELECT repeat(' ', 5) FROM t",
        );
    }

    #[test]
    fn redshift_sha2_256() {
        assert_transpiles(
            "SELECT SHA2(col, 256) FROM t",
            SourceDialect::Redshift,
            "SELECT sha256(col) FROM t",
        );
    }

    #[test]
    fn redshift_is_valid_json() {
        assert_transpiles(
            "SELECT IS_VALID_JSON(col) FROM t",
            SourceDialect::Redshift,
            "SELECT json_valid(col) FROM t",
        );
    }

    // New array function tests
    #[test]
    fn trino_array_except() {
        assert_transpiles(
            "SELECT array_except(a, b) FROM t",
            SourceDialect::Trino,
            "SELECT list_except(a, b) FROM t",
        );
    }

    #[test]
    fn trino_array_sum() {
        assert_transpiles(
            "SELECT array_sum(nums) FROM t",
            SourceDialect::Trino,
            "SELECT list_sum(nums) FROM t",
        );
    }

    #[test]
    fn trino_array_average() {
        assert_transpiles(
            "SELECT array_average(nums) FROM t",
            SourceDialect::Trino,
            "SELECT list_avg(nums) FROM t",
        );
    }

    #[test]
    fn trino_array_has() {
        assert_transpiles(
            "SELECT array_has(arr, 42) FROM t",
            SourceDialect::Trino,
            "SELECT list_contains(arr, 42) FROM t",
        );
    }

    #[test]
    fn trino_array_has_all() {
        let result = crate::transpile(
            "SELECT array_has_all(arr, ARRAY[1, 2]) FROM t",
            SourceDialect::Trino,
        )
        .unwrap();
        assert!(result.contains("list_intersect"), "Got: {result}");
        assert!(result.contains("len"), "Got: {result}");
    }

    #[test]
    fn trino_array_has_any() {
        let result = crate::transpile(
            "SELECT array_has_any(arr, ARRAY[1, 2]) FROM t",
            SourceDialect::Trino,
        )
        .unwrap();
        assert!(result.contains("list_intersect"), "Got: {result}");
        assert!(result.contains("> 0"), "Got: {result}");
    }

    // New JSON function tests
    #[test]
    fn trino_json_parse() {
        assert_transpiles(
            "SELECT json_parse(s) FROM t",
            SourceDialect::Trino,
            "SELECT CAST(s AS JSON) FROM t",
        );
    }

    #[test]
    fn trino_json_format() {
        assert_transpiles(
            "SELECT json_format(j) FROM t",
            SourceDialect::Trino,
            "SELECT CAST(j AS VARCHAR) FROM t",
        );
    }

    #[test]
    fn trino_json_array_get() {
        assert_transpiles(
            "SELECT json_array_get(j, 2) FROM t",
            SourceDialect::Trino,
            "SELECT json_extract_string(j, '$[2]') FROM t",
        );
    }

    #[test]
    fn trino_json_object_keys() {
        assert_transpiles(
            "SELECT json_object_keys(j) FROM t",
            SourceDialect::Trino,
            "SELECT json_keys(j) FROM t",
        );
    }

    #[test]
    fn trino_json_array_length() {
        assert_transpiles(
            "SELECT json_array_length(j) FROM t",
            SourceDialect::Trino,
            "SELECT json_array_length(j) FROM t",
        );
    }

    #[test]
    fn redshift_json_typeof() {
        assert_transpiles(
            "SELECT JSON_TYPEOF(col) FROM t",
            SourceDialect::Redshift,
            "SELECT json_type(col) FROM t",
        );
    }

    #[test]
    fn redshift_json_serialize() {
        assert_transpiles(
            "SELECT JSON_SERIALIZE(j) FROM t",
            SourceDialect::Redshift,
            "SELECT CAST(j AS VARCHAR) FROM t",
        );
    }

    #[test]
    fn redshift_json_deserialize() {
        assert_transpiles(
            "SELECT JSON_DESERIALIZE(s) FROM t",
            SourceDialect::Redshift,
            "SELECT CAST(s AS JSON) FROM t",
        );
    }

    #[test]
    fn redshift_array_concat() {
        assert_transpiles(
            "SELECT array_concat(a, b) FROM t",
            SourceDialect::Redshift,
            "SELECT list_concat(a, b) FROM t",
        );
    }

    #[test]
    fn redshift_dateadd_to_interval() {
        let result = crate::transpile(
            "SELECT DATEADD(month, 3, created_at) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("INTERVAL"),
            "Expected INTERVAL in: {result}"
        );
        assert!(result.contains("MONTH"), "Expected MONTH in: {result}");
        assert!(result.contains("+"), "Expected + operator in: {result}");
    }

    #[test]
    fn redshift_dateadd_day() {
        let result = crate::transpile(
            "SELECT DATEADD(day, 7, order_date) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("INTERVAL"),
            "Expected INTERVAL in: {result}"
        );
        assert!(result.contains("DAY"), "Expected DAY in: {result}");
    }

    #[test]
    fn redshift_datediff_to_quoted() {
        let result = crate::transpile(
            "SELECT DATEDIFF(day, start_date, end_date) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("date_diff"),
            "Expected date_diff in: {result}"
        );
        assert!(
            result.contains("'day'"),
            "Expected quoted 'day' in: {result}"
        );
    }

    #[test]
    fn redshift_datediff_month() {
        let result = crate::transpile(
            "SELECT DATEDIFF(month, hire_date, term_date) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("date_diff"),
            "Expected date_diff in: {result}"
        );
        assert!(
            result.contains("'month'"),
            "Expected quoted 'month' in: {result}"
        );
    }

    // Format string conversion tests

    #[test]
    fn trino_format_datetime_java_to_strftime() {
        assert_transpiles(
            "SELECT format_datetime(ts, 'yyyy-MM-dd HH:mm:ss') FROM t",
            SourceDialect::Trino,
            "SELECT strftime(ts, '%Y-%m-%d %H:%M:%S') FROM t",
        );
    }

    #[test]
    fn trino_format_datetime_preserves_strftime() {
        // Already strftime-style format should not be double-converted
        assert_transpiles(
            "SELECT format_datetime(ts, '%Y-%m-%d') FROM t",
            SourceDialect::Trino,
            "SELECT strftime(ts, '%Y-%m-%d') FROM t",
        );
    }

    #[test]
    fn trino_date_parse_java_to_strftime() {
        assert_transpiles(
            "SELECT date_parse(s, 'yyyy/MM/dd') FROM t",
            SourceDialect::Trino,
            "SELECT strptime(s, '%Y/%m/%d') FROM t",
        );
    }

    #[test]
    fn redshift_to_char_pg_to_strftime() {
        assert_transpiles(
            "SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t",
            SourceDialect::Redshift,
            "SELECT strftime(ts, '%Y-%m-%d %H:%M:%S') FROM t",
        );
    }

    #[test]
    fn redshift_to_date_pg_to_strftime() {
        let result = crate::transpile(
            "SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD') FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("strptime"),
            "Expected strptime in: {result}"
        );
        assert!(
            result.contains("%Y-%m-%d"),
            "Expected converted format in: {result}"
        );
        assert!(result.contains("CAST"), "Expected CAST in: {result}");
    }

    #[test]
    fn redshift_to_timestamp_pg_to_strftime() {
        assert_transpiles(
            "SELECT TO_TIMESTAMP(s, 'YYYY-MM-DD') FROM t",
            SourceDialect::Redshift,
            "SELECT strptime(s, '%Y-%m-%d') FROM t",
        );
    }

    #[test]
    fn trino_format_datetime_with_literal() {
        assert_transpiles(
            "SELECT format_datetime(ts, 'yyyy''T''HH:mm:ss') FROM t",
            SourceDialect::Trino,
            "SELECT strftime(ts, '%YT%H:%M:%S') FROM t",
        );
    }

    // New function mapping tests

    #[test]
    fn trino_from_hex() {
        assert_transpiles(
            "SELECT from_hex(s) FROM t",
            SourceDialect::Trino,
            "SELECT unhex(s) FROM t",
        );
    }

    #[test]
    fn trino_rand() {
        assert_transpiles(
            "SELECT rand() FROM t",
            SourceDialect::Trino,
            "SELECT random() FROM t",
        );
    }

    #[test]
    fn trino_date_format() {
        assert_transpiles(
            "SELECT date_format(ts, 'yyyy-MM-dd') FROM t",
            SourceDialect::Trino,
            "SELECT strftime(ts, '%Y-%m-%d') FROM t",
        );
    }

    #[test]
    fn trino_at_timezone() {
        assert_transpiles(
            "SELECT at_timezone(ts, 'US/Eastern') FROM t",
            SourceDialect::Trino,
            "SELECT ts AT TIME ZONE 'US/Eastern' FROM t",
        );
    }

    #[test]
    fn redshift_months_between() {
        let result = crate::transpile(
            "SELECT MONTHS_BETWEEN(end_date, start_date) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("datediff"),
            "Expected datediff in: {result}"
        );
        assert!(result.contains("'month'"), "Expected 'month' in: {result}");
    }

    #[test]
    fn redshift_add_months() {
        let result = crate::transpile(
            "SELECT ADD_MONTHS(start_date, 6) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(
            result.contains("INTERVAL"),
            "Expected INTERVAL in: {result}"
        );
        assert!(result.contains("MONTH"), "Expected MONTH in: {result}");
        assert!(result.contains("+"), "Expected + in: {result}");
    }

    #[test]
    fn trino_to_unixtime() {
        assert_transpiles(
            "SELECT to_unixtime(ts) FROM t",
            SourceDialect::Trino,
            "SELECT epoch(ts) FROM t",
        );
    }

    #[test]
    fn trino_parse_datetime() {
        assert_transpiles(
            "SELECT parse_datetime(s, 'yyyy-MM-dd') FROM t",
            SourceDialect::Trino,
            "SELECT strptime(s, '%Y-%m-%d') FROM t",
        );
    }

    #[test]
    fn trino_with_timezone() {
        assert_transpiles(
            "SELECT with_timezone(ts, 'UTC') FROM t",
            SourceDialect::Trino,
            "SELECT ts AT TIME ZONE 'UTC' FROM t",
        );
    }

    #[test]
    fn trino_current_timezone() {
        let result = crate::transpile("SELECT current_timezone()", SourceDialect::Trino).unwrap();
        assert!(
            result.contains("current_setting"),
            "Expected current_setting in: {result}"
        );
        assert!(
            result.contains("TimeZone"),
            "Expected TimeZone in: {result}"
        );
    }

    #[test]
    fn trino_map_agg() {
        let result = crate::transpile("SELECT map_agg(k, v) FROM t", SourceDialect::Trino).unwrap();
        assert!(result.contains("map("), "Expected map( in: {result}");
        assert!(result.contains("list(k)"), "Expected list(k) in: {result}");
        assert!(result.contains("list(v)"), "Expected list(v) in: {result}");
    }

    #[test]
    fn redshift_strtol_base16() {
        let result =
            crate::transpile("SELECT STRTOL(hex_str, 16) FROM t", SourceDialect::Redshift).unwrap();
        assert!(result.contains("CASE"), "Expected CASE in: {result}");
        assert!(result.contains("0x"), "Expected 0x prefix in: {result}");
        assert!(result.contains("BIGINT"), "Expected BIGINT in: {result}");
    }

    #[test]
    fn redshift_strtol_base10() {
        let result =
            crate::transpile("SELECT STRTOL('42', 10) FROM t", SourceDialect::Redshift).unwrap();
        assert!(result.contains("CASE"), "Expected CASE in: {result}");
        assert!(result.contains("BIGINT"), "Expected BIGINT in: {result}");
    }

    #[test]
    fn redshift_strtol_dynamic_base() {
        // Base as a column reference — should still produce CASE
        let result = crate::transpile(
            "SELECT STRTOL(val, base_col) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(result.contains("CASE"), "Expected CASE in: {result}");
        assert!(
            result.contains("base_col"),
            "Expected base_col in: {result}"
        );
    }

    #[test]
    fn redshift_ratio_to_report() {
        let result = crate::transpile(
            "SELECT RATIO_TO_REPORT(amount) OVER (PARTITION BY dept) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        assert!(result.contains("/"), "Expected division in: {result}");
        assert!(result.contains("SUM"), "Expected SUM in: {result}");
        assert!(
            result.contains("PARTITION BY"),
            "Expected PARTITION BY preserved in: {result}"
        );
    }

    #[test]
    fn redshift_date_part_unquoted() {
        let result = crate::transpile(
            "SELECT DATE_PART(year, created_at) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("date_part('year'"),
            "Expected quoted 'year' in: {result}"
        );
    }

    #[test]
    fn redshift_date_part_already_quoted() {
        let result = crate::transpile(
            "SELECT DATE_PART('month', created_at) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("date_part('month'"),
            "Expected date_part in: {result}"
        );
    }

    #[test]
    fn redshift_date_trunc_unquoted() {
        let result = crate::transpile(
            "SELECT DATE_TRUNC(month, created_at) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("date_trunc('month'"),
            "Expected quoted 'month' in: {result}"
        );
    }

    #[test]
    fn redshift_date_trunc_already_quoted() {
        let result = crate::transpile(
            "SELECT DATE_TRUNC('day', ts) FROM t",
            SourceDialect::Redshift,
        )
        .unwrap();
        let lower = result.to_lowercase();
        assert!(
            lower.contains("date_trunc('day'"),
            "Expected date_trunc in: {result}"
        );
    }
}
