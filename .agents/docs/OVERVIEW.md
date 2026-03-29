# Project Overview

## Summary

papera is a SQL compatibility layer that transpiles Trino, Redshift, and Hive SQL into DuckDB-compatible SQL. It is intended to be usable both as a library embedded in other Rust programs and as an optional CLI for stdin-to-stdout SQL rewriting.

The project's main value is preserving useful source-dialect behavior while making the compatibility boundary explicit. Simple name rewrites are supported where possible, but some features require structural AST rewrites, and some conversions remain intentionally opt-in or unsupported when DuckDB has no safe direct equivalent.

## Supported Dialects

- Trino, parsed with `GenericDialect`
- Redshift, parsed with `RedshiftSqlDialect`
- Hive, parsed with `HiveDialect`

Hive is treated as a first-class dialect rather than as a Trino variant because parser support and DDL syntax differ materially.

## Primary Interfaces

- Library entrypoints: `papera::transpile` and `papera::transpile_with_options`
- Core API types: `papera::SourceDialect`, `papera::TranspileOptions`, `papera::ExternalTableBehavior`, `papera::IcebergTableBehavior`
- Optional CLI: `papera <trino|redshift|hive>`

The crate is library-first. The CLI is feature-gated and is not built by default.

## Compatibility Model

- papera parses source SQL with sqlparser-rs, rewrites the AST, and emits DuckDB SQL.
- Compatibility work spans function mapping, type normalization, DDL rewriting, SHOW emulation, UNNEST normalization, and Hive lateral expansion.
- External-table and Iceberg rewrites are opt-in through `TranspileOptions` and default to `Error`.
- Some mappings are approximations rather than exact semantic matches, especially where DuckDB lacks a direct equivalent.

## Project Boundaries

- The project is designed around SQL transpilation, not around emulating every source-dialect feature.
- Parser behavior is part of the effective feature boundary. Some source syntax cannot be supported cleanly until the parser exposes a usable AST.
- Execution behavior in DuckDB is the final correctness target, so string-level rewrite success alone is not considered sufficient evidence of compatibility.

## Repository Landmarks

- `src/lib.rs`: crate root API
- `src/main.rs`: CLI entrypoint
- `src/dialect/`: dialect-specific parsing and orchestration
- `src/transpiler/`: shared rewrite traversal and dialect contract
- `src/transforms/`: rewrite implementations for functions, types, DDL, SHOW, UNNEST, and lateral handling
- `tests/`: string-level and DuckDB execution-level regression coverage
