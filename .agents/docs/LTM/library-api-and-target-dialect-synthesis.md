# Library API and Target Dialect Synthesis

## Summary

papera's long-term caller-facing contract is centered on a small crate-root API, explicit rewrite options, and a growing set of target-specific compatibility controls. The packaging refactor established the library-first boundary, `SerdeClassResolver` added a sanctioned extensibility hook for external-table reader inference, and `TargetDialect::DataFusion` introduced a second output backend without changing the default DuckDB behavior.

The durable lesson across these documents is that public API growth should stay narrow and intentional. New options and exports are valuable when they preserve the internal rewrite system's flexibility, but they also create semver obligations and require clear guidance around target-specific limits and unsupported paths.

## Included Documents

| Document | Focus |
|----------|-------|
| [library-packaging-and-public-api.md](./library-packaging-and-public-api.md) | Library-first crate packaging, CLI feature gating, and crate-root public API boundaries |
| [serde-class-resolver.md](./serde-class-resolver.md) | Public callback for caller-defined Hive / Trino SerDe class to reader-function mapping |
| [datafusion-target-dialect.md](./datafusion-target-dialect.md) | `TargetDialect::DataFusion` support, target threading, execution-backed audit results, and remaining unsupported paths |

## Stable Knowledge

- The crate is intentionally library-first: `cargo build` targets the library by default, while the CLI stays behind the `cli` feature.
- The stable crate-root API is deliberately small and includes helpers such as `papera::transpile`, `papera::transpile_with_options`, `SourceDialect`, `TranspileOptions`, `Error`, and `Result`.
- Public options are the main compatibility boundary. Behavior-changing rewrites and target selection should be expressed through `TranspileOptions` rather than by exposing internal transform modules.
- `SerdeClassResolver` is a public extension point for reader inference. It is part of the semver-facing API and lets callers handle custom SerDe classes without forking the library.
- Resolver behavior is layered: caller-provided resolution runs first, then built-in SerDe inference runs if the callback returns `None`.
- `TargetDialect` adds a second output dimension while preserving backward compatibility by defaulting to `DuckDB`.
- DataFusion support is selective rather than universal. Many Trino and Redshift functions pass through or remap cleanly, but several seemingly compatible functions still need custom rewrites or explicit `Error::Unsupported`.
- DataFusion target work needs execution-backed validation. String-level transpilation tests can miss target-specific wrong-name leakage and incorrect assumptions about actual function availability.
- DataFusion target users should keep external-table and Iceberg rewrites disabled unless native DataFusion equivalents are implemented, because the current reader-backed view generation is DuckDB-specific.
- The current durable DataFusion limits are mostly explicit unsupported paths, including `to_utf8`, `from_utf8`, `json_object_keys`, `json_extract_scalar`, `json_extract`, `json_array_get`, `arbitrary`, `filter`, `is_finite`, `is_infinite`, `map_agg`, Redshift `json_extract_path_text`, and Redshift `strtol`.

## Operational Guidance

When adding new user-visible behavior, start by deciding whether it belongs in the stable API at all. Prefer crate-root exports and `TranspileOptions` fields only for capabilities that callers genuinely need to configure or compose.

For extensibility work, preserve the current pattern:

- Keep internal rewrite machinery under `pub(crate)` unless there is a strong external-use case.
- Add caller hooks at narrow decision points, as `SerdeClassResolver` does for reader selection.
- Make fallthrough behavior explicit so custom hooks can extend built-ins rather than replace them accidentally.

For target-dialect work, use these heuristics:

- Treat `TargetDialect` as a first-class branch in function, type, DDL, and SHOW rewrites.
- Do not reuse DuckDB-specific custom handlers on the DataFusion path unless the generated SQL is verified to be target-correct.
- Prefer execution-backed tests in `tests/datafusion_integration.rs` when introducing or changing DataFusion behavior.
- Prefer returning `Error::Unsupported` over silently emitting SQL that only looks plausible for the selected target.
- Add regression tests for both the new target behavior and the default DuckDB path so backward compatibility stays visible.
- Be careful with custom arithmetic rewrites for DataFusion date functions. Missing `Expr::Nested` wrappers can change operator precedence in serialized SQL.

## Files

- `Cargo.toml`: library target, CLI feature gate, and packaging metadata
- `src/lib.rs`: crate-root exports, `TranspileOptions`, and `SerdeClassResolver`
- `src/main.rs`: optional CLI entrypoint
- `src/dialect/mod.rs`: `TargetDialect`
- `src/transpiler/rewrite.rs`: target-aware expression rewriter construction
- `src/transforms/functions.rs`: target-specific function mappings and the main DataFusion risk area
- `src/transforms/types.rs`: target-specific type rewriting
- `src/transforms/ddl.rs`: external-table, Iceberg, and SerDe reader selection behavior
- `src/transforms/show.rs`: DuckDB versus DataFusion SHOW handling
- `tests/datafusion_integration.rs`: execution-backed verification for DataFusion 52.4.0 behavior
- `.agents/docs/TODO.md`: tracked open follow-up items that remain outside the completed DataFusion mapping audit

## Tests

- `cargo build`
- `cargo build --features cli`
- `cargo test`
- `cargo test --test integration`
- `cargo test --test datafusion_integration`

Recorded coverage in the source docs includes packaging-regression checks with the existing suite, SerDe resolver unit and integration tests, string-level target rewrite checks, and 51 execution-backed DataFusion integration tests that validate actual runtime support rather than only emitted SQL text.

## Pitfalls

- Every new public export expands the long-term API contract. Avoid exposing internal transform helpers just because they are convenient for one caller.
- `SerdeClassResolver` short-circuits built-in inference when it returns `Some(...)`, so an overly broad callback can shadow correct built-in mappings.
- DataFusion support can look more complete than it really is if validation stops at transpiled SQL strings. The runtime surface of DataFusion 52 is narrower than the naming overlap with Trino suggests.
- Reader-backed external-table and Iceberg rewrites are not portable across targets. They are safe to describe as DuckDB compatibility features, not as generic multi-target behavior.
- Passing string-comparison tests is not enough for target-dialect work. Incorrect target-specific function names and unsupported runtime assumptions can still survive unless behavior is validated directly.
