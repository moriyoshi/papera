# Configurable SerDe Class Resolver

## Summary

`SerdeClassResolver` is a user-supplied callback in `TranspileOptions` that lets callers override or extend the built-in Hive SerDe class → DuckDB reader function mapping. Without it, unknown SerDe classes cause an `Unsupported` error. With it, callers can map private or custom SerDe classes to any reader function ( `read_parquet`, `read_csv`, etc. ) without patching the library.

## Key Facts

- Added to `TranspileOptions` as `serde_class_resolver: Option<SerdeClassResolver>` ( defaults to `None` ).
- The resolver is called before built-in substring matching; returning `None` falls through to built-in logic.
- It fires for both `SourceDialect::Hive` and `SourceDialect::Trino` when the SQL contains `CREATE EXTERNAL TABLE ... ROW FORMAT SERDE`.
- `SerdeClassResolver` is a public API export — it is part of the stable library surface.
- Wrapped in `Arc<dyn Fn(...) + Send + Sync>` so it is `Clone` via ref-count bump and safe to pass across threads.

## Details

### Motivation

`reader_from_serde_class` previously covered common classes via substring matching
( `ParquetHiveSerDe` → `read_parquet`, `OrcSerde` → `read_orc`, etc. ) and returned
`Unsupported` for anything unrecognized. Callers with custom or private SerDe classes
had no way to extend this mapping without forking the library.

### Type definition

```rust
// src/lib.rs
#[derive(Clone)]
pub struct SerdeClassResolver(Arc<dyn Fn(&str) -> Option<String> + Send + Sync + 'static>);

impl SerdeClassResolver {
    pub fn new(f: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }
    pub(crate) fn resolve(&self, class: &str) -> Option<String> {
        (self.0)(class)
    }
}
```

### Fallthrough chain

`determine_reader_function` ( `src/transforms/ddl.rs` ) calls
`reader_from_serde_class(class, opts)`, which follows this order:

1. If `opts.serde_class_resolver` is `Some(resolver)`, call `resolver.resolve(class)`.
2. If it returns `Some(reader_fn)`, use that.
3. If it returns `None` ( or there is no resolver ), call `infer_reader_from_serde_class(class)`.
4. Built-in inference matches via `contains()` on the lower-cased class name:
   - `parquethiveserde` → `read_parquet`
   - `orcserde` → `read_orc`
   - `jsonserde` or `hcatalogicjsonserde` or `openjsonserde` → `read_json`
   - `opencsvserdeproperties` or `lazysimpleserdeproperties` → `read_csv`
   - `regexserde` → `Unsupported` ( no DuckDB equivalent )
   - anything else → `Unsupported` with class name in the message

### Function signature change

Before the resolver was added, `determine_reader_function` and
`reader_from_serde_class` returned `Result<&'static str>`. After adding it, both
return `Result<String>` because the user-supplied resolver can return any heap
string.

### Example usage

```rust
let opts = TranspileOptions {
    external_table: ExternalTableBehavior::MapToView,
    serde_class_resolver: Some(SerdeClassResolver::new(|class| {
        if class.eq_ignore_ascii_case("com.example.CustomSerDe") {
            Some("read_parquet".to_string())
        } else {
            None
        }
    })),
    ..Default::default()
};
```

## Files

- `src/lib.rs`: `SerdeClassResolver` type, `TranspileOptions::serde_class_resolver`
- `src/transforms/ddl.rs`: `determine_reader_function`, `reader_from_serde_class`,
  `infer_reader_from_serde_class`

## Test Coverage

- `src/transforms/ddl.rs` unit tests:
  - `hive_external_table_serde_custom_resolver` — resolver matches exact class name
  - `hive_external_table_serde_custom_resolver_fallthrough` — resolver returns `None`, built-in logic returns `Unsupported`
- `tests/integration.rs`:
  - `hive_serde_custom_resolver_overrides_unknown` ( Hive dialect )
  - `trino_hive_style_serde_custom_resolver` ( Trino dialect with Hive-style DDL )
- `src/lib.rs` doctest on `SerdeClassResolver`

## Pitfalls

- The resolver receives the raw class string exactly as written in the SQL ( including original casing ). Case-insensitive matching is the caller's responsibility.
- Returning `Some(reader_fn)` short-circuits the built-in map entirely, including known classes. This allows overrides but also means a poorly-written resolver can shadow correct built-in mappings.
- `SerdeClassResolver` is `pub` — once callers start using it, its API shape is part of the semver contract.
- `external_table: ExternalTableBehavior::MapToView` must still be set for the resolver to fire. With the default `ExternalTableBehavior::Error`, external tables are rejected before SerDe resolution happens.
