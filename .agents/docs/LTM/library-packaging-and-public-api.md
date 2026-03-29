# Library Packaging and Public API

## Summary

papera was restructured from a standalone application into a library-first crate with an optional CLI. The packaging change narrows the public API to stable entrypoints and keeps transformation internals out of the semver-facing surface.

This split matters because the crate is now intended to be embedded by callers, not only invoked from the command line. The resulting API is small, explicit, and centered on dialect selection plus configurable rewrite options.

## Key Facts

- `Cargo.toml` declares both a library target and a feature-gated CLI binary.
- The CLI now requires the `cli` feature; default builds produce the library only.
- `src/transforms` was intentionally reduced to `pub(crate)` visibility.
- `SourceDialect` and `Transpiler` are re-exported at the crate root for ergonomic library usage.
- Existing tests continued to pass without test code changes after the packaging cleanup.

## Details

### Packaging structure

The crate moved to a library-first layout with these packaging decisions:

- An explicit `[lib]` section declares the library target.
- The CLI lives under `[[bin]]`.
- The CLI binary is guarded by `required-features = ["cli"]`.
- `[features]` defines the `cli` feature.
- Package metadata includes license, keywords, and categories.

This means:

- `cargo build` builds the library by default.
- CLI-oriented workflows must opt in with `--features cli`.

### Public API surface

The intended public surface recorded in the journal is:

| Item | Public path |
|------|-------------|
| Transpile helper | `papera::transpile` |
| Configurable transpile helper | `papera::transpile_with_options` |
| Source dialect selector | `papera::SourceDialect` |
| Transpiler trait | `papera::Transpiler` |
| Options type | `papera::TranspileOptions` |
| External table behavior | `papera::ExternalTableBehavior` |
| Iceberg table behavior | `papera::IcebergTableBehavior` |
| Error and result types | `papera::Error`, `papera::Result` |

The journal also notes that concrete dialect implementations remain available under `papera::dialect::{TrinoDialect, RedshiftDialect, HiveTranspileDialect}`.

### Internal boundary

`src/transforms` contains implementation details such as:

- function mapping tables
- type rewrites
- DDL and DML transforms
- SHOW handling
- UNNEST and lateral normalization

Keeping this module `pub(crate)` avoids exposing unstable internal mechanics as part of the library contract.

## Files

- `Cargo.toml`: target declarations, features, and package metadata
- `src/lib.rs`: crate root API and re-exports
- `src/main.rs`: CLI entrypoint
- `src/transforms/`: internal rewrite implementation kept outside the public API contract

## Test Coverage

The journal records that all 150 existing tests still passed after the packaging cleanup, with no required test changes. That indicates the public APIs already used by tests stayed stable across the refactor.

Useful commands:

- `cargo build`
- `cargo build --features cli`
- `cargo test`

## Pitfalls

- New public exports should be added deliberately because they expand the long-term API contract.
- CLI-specific dependencies or behavior should stay behind the `cli` feature to preserve the lightweight library default.
- Internal transform modules may still feel reusable, but exposing them would couple downstream users to unstable implementation details.
