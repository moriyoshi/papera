use sqlparser::ast::{
    ArrayElemTypeDef, CharacterLength, DataType, Ident, ObjectName, StructBracketKind, StructField,
};

use crate::Result;
use crate::dialect::SourceDialect;
use crate::error::Error;

/// Rewrite a DataType from source dialect to DuckDB-compatible form.
/// Recurses into nested types (e.g., ARRAY element types, MAP key/value types).
pub fn rewrite_data_type(dt: &mut DataType, dialect: SourceDialect) -> Result<()> {
    match dialect {
        SourceDialect::Trino => rewrite_trino_type(dt),
        SourceDialect::Redshift => rewrite_redshift_type(dt),
        // Hive types are similar to Trino (both use ROW, ARRAY, MAP, etc.)
        SourceDialect::Hive => rewrite_trino_type(dt),
    }
}

// ---------------------------------------------------------------------------
// Trino → DuckDB
// ---------------------------------------------------------------------------

fn rewrite_trino_type(dt: &mut DataType) -> Result<()> {
    match dt {
        // ROW(a INTEGER, b VARCHAR) → STRUCT(a INTEGER, b VARCHAR)
        // Parsed by GenericDialect as Custom(ObjectName(["ROW"]), ["a", "INTEGER", "b", "VARCHAR"])
        DataType::Custom(name, modifiers) if is_name(name, "row") => {
            let fields = parse_row_modifiers(modifiers)?;
            *dt = DataType::Struct(fields, StructBracketKind::Parentheses);
            // Recurse into struct field types
            if let DataType::Struct(fields, _) = dt {
                for field in fields.iter_mut() {
                    rewrite_trino_type(&mut field.field_type)?;
                }
            }
        }

        // ARRAY(T) is parsed by sqlparser as Array(Parenthesis(T))
        // DuckDB prefers T[] syntax → Array(SquareBracket(T, None))
        DataType::Array(ArrayElemTypeDef::Parenthesis(inner)) => {
            rewrite_trino_type(inner)?;
            let inner_owned = std::mem::replace(inner.as_mut(), DataType::Unspecified);
            *dt = DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(inner_owned), None));
        }
        // Also handle ARRAY<T> → T[]
        DataType::Array(ArrayElemTypeDef::AngleBracket(inner)) => {
            rewrite_trino_type(inner)?;
            let inner_owned = std::mem::replace(inner.as_mut(), DataType::Unspecified);
            *dt = DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(inner_owned), None));
        }
        DataType::Array(ArrayElemTypeDef::SquareBracket(inner, _)) => {
            rewrite_trino_type(inner)?;
        }

        // MAP(K, V) → MAP(K, V) — already parsed as DataType::Map, passthrough
        // but recurse into key and value types
        DataType::Map(key, value) => {
            rewrite_trino_type(key)?;
            rewrite_trino_type(value)?;
        }

        // Trino VARBINARY → DuckDB BLOB
        DataType::Varbinary(_) => {
            *dt = DataType::Blob(None);
        }

        // Trino IPADDRESS → DuckDB VARCHAR
        DataType::Custom(name, modifiers) if is_name(name, "ipaddress") && modifiers.is_empty() => {
            *dt = DataType::Varchar(None);
        }

        // Trino TINYINT → DuckDB TINYINT (passthrough)
        // Trino REAL → DuckDB REAL (passthrough)
        // Most standard types pass through unchanged
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Redshift → DuckDB
// ---------------------------------------------------------------------------

fn rewrite_redshift_type(dt: &mut DataType) -> Result<()> {
    match dt {
        // VARCHAR(MAX) → VARCHAR (unbounded)
        DataType::Varchar(Some(CharacterLength::Max)) => {
            *dt = DataType::Varchar(None);
        }
        DataType::CharacterVarying(Some(CharacterLength::Max)) => {
            *dt = DataType::Varchar(None);
        }
        DataType::Nvarchar(Some(CharacterLength::Max)) => {
            *dt = DataType::Varchar(None);
        }

        // SUPER → JSON
        DataType::Custom(name, modifiers) if is_name(name, "super") && modifiers.is_empty() => {
            *dt = DataType::JSON;
        }

        // HLLSKETCH → unsupported
        DataType::Custom(name, modifiers) if is_name(name, "hllsketch") && modifiers.is_empty() => {
            return Err(Error::Unsupported(
                "Redshift HLLSKETCH type has no DuckDB equivalent".to_string(),
            ));
        }

        // GEOMETRY → unsupported (DuckDB has spatial extension but different type system)
        DataType::Custom(name, modifiers) if is_name(name, "geometry") && modifiers.is_empty() => {
            return Err(Error::Unsupported(
                "Redshift GEOMETRY type has no direct DuckDB equivalent".to_string(),
            ));
        }

        // Redshift VARBINARY → DuckDB BLOB
        DataType::Varbinary(_) => {
            *dt = DataType::Blob(None);
        }

        // TIMETZ / TIMESTAMPTZ pass through (DuckDB supports these)
        // Standard types pass through
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_name(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

/// Parse the raw modifier strings from a Custom("ROW", [...]) into StructFields.
/// The modifiers alternate between field name and field type strings:
/// ["a", "INTEGER", "b", "VARCHAR"] → [StructField("a", INTEGER), StructField("b", VARCHAR)]
fn parse_row_modifiers(modifiers: &[String]) -> Result<Vec<StructField>> {
    if !modifiers.len().is_multiple_of(2) {
        return Err(Error::Unsupported(format!(
            "Cannot parse ROW type modifiers: {modifiers:?}"
        )));
    }

    let mut fields = Vec::new();
    for chunk in modifiers.chunks(2) {
        let field_name = &chunk[0];
        let type_str = &chunk[1];
        let field_type = parse_type_string(type_str)?;
        fields.push(StructField {
            field_name: Some(Ident::new(field_name)),
            field_type,
            options: None,
        });
    }
    Ok(fields)
}

/// Parse a simple type name string into a DataType.
fn parse_type_string(s: &str) -> Result<DataType> {
    let upper = s.trim().to_uppercase();
    let dt = match upper.as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" | "INT1" => DataType::TinyInt(None),
        "SMALLINT" | "INT2" => DataType::SmallInt(None),
        "INTEGER" | "INT" | "INT4" => DataType::Integer(None),
        "BIGINT" | "INT8" => DataType::BigInt(None),
        "REAL" | "FLOAT4" => DataType::Real,
        "DOUBLE" | "FLOAT8" | "DOUBLE PRECISION" => {
            DataType::Double(sqlparser::ast::ExactNumberInfo::None)
        }
        "VARCHAR" | "STRING" => DataType::Varchar(None),
        "TEXT" => DataType::Text,
        "DATE" => DataType::Date,
        "TIMESTAMP" => DataType::Timestamp(None, sqlparser::ast::TimezoneInfo::None),
        "JSON" => DataType::JSON,
        "BLOB" | "BYTEA" => DataType::Blob(None),
        "UUID" => DataType::Uuid,
        other => DataType::Custom(ObjectName::from(vec![Ident::new(other)]), vec![]),
    };
    Ok(dt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redshift_varchar_max() {
        let mut dt = DataType::Varchar(Some(CharacterLength::Max));
        rewrite_data_type(&mut dt, SourceDialect::Redshift).unwrap();
        assert_eq!(dt, DataType::Varchar(None));
    }

    #[test]
    fn redshift_super_to_json() {
        let mut dt = DataType::Custom(ObjectName::from(vec![Ident::new("SUPER")]), vec![]);
        rewrite_data_type(&mut dt, SourceDialect::Redshift).unwrap();
        assert_eq!(dt, DataType::JSON);
    }

    #[test]
    fn trino_varbinary_to_blob() {
        let mut dt = DataType::Varbinary(None);
        rewrite_data_type(&mut dt, SourceDialect::Trino).unwrap();
        assert_eq!(dt, DataType::Blob(None));
    }

    #[test]
    fn trino_row_to_struct() {
        let mut dt = DataType::Custom(
            ObjectName::from(vec![Ident::new("ROW")]),
            vec![
                "a".to_string(),
                "INTEGER".to_string(),
                "b".to_string(),
                "VARCHAR".to_string(),
            ],
        );
        rewrite_data_type(&mut dt, SourceDialect::Trino).unwrap();
        match &dt {
            DataType::Struct(fields, StructBracketKind::Parentheses) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].field_name.as_ref().unwrap().value, "a");
                assert_eq!(fields[1].field_name.as_ref().unwrap().value, "b");
            }
            other => panic!("Expected Struct, got {other:?}"),
        }
    }
}
