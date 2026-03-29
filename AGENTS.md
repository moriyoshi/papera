# Documents for both humans and coding agents

* [README.md](./README.md)

# Documents for coding agents

* [./.agents/docs/OVERVIEW.md](./.agents/docs/OVERVIEW.md) ... project overview.
* [./.agents/docs/ARCHITECTURE.md](./.agents/docs/ARCHITECTURE.md) ... system architecture.
* [./.agents/docs/JOURNAL.md](./.agents/docs/JOURNAL.md) ... findings insights, and peer code review history.
* [./.agents/docs/LTM/INDEX.md](./.agents/docs/LTM/INDEX.md) ... long-term memory index for durable project knowledge under `./.agents/docs/LTM/`.
* [./.agents/docs/TODO.md](./.agents/docs/TODO.md) ... open to-do items extracted from JOURNAL.md during `good-sleep` consolidation. Check and update this file when picking up or finishing work.

# Rules and protocols

## File Management

* When you'd make summary documents for your work, be sure to write them under `./.agents/docs`, not under `/tmp`.
* Temporary files should be created under `./.agents/tmp`, not under `/tmp`.
* ❌ Do not randomly create a binary under the version controlled directory through `go build ./cmd/s3router`. Always put it under `./.agents/tmp`.
* ❌ Never delete user files without permission. Only safe to delete: files YOU created in THIS session that are in `./.agents/tmp/`. Always ask first if unsure. Assume all pre-existing files belong to user.

## Documentation

* Try to write your work summary to one of the existing documents.
* ❌ Avoid editing any existing sections of JOURNAL.md. You should rather just append texts to it.

## Testing

* Make sure that regression tests are ready for your fix.
* ❌ You shouldn't run the entire integration test suites at once. Or if you can spare them 2+ minutes, be patient with it. You should always specify `--maxfail=n` (n should be a number less than 10), and also be sure to specify `--lf` as well when you want to run the last failing tests.

## Python

* If there's a `pyproject.toml` file, try to run the tests with `uv run pytest ...` and arbitrary scripts with `uv run python ...`.
  * ❌ If there's no `pyproject.toml`, never run a bare `pip install` out of a venv. Always use `uv pip ...` in combination with `uv venv`.
* For typed Python tests:
  * Keep fixture return types as `Iterator[Type]` for yield fixtures.
  * Avoid `dict[str, object]` and `list[object]` in annotations; use `dict[str, Any]`/`list[Any]` for mixed payloads and real TypedDicts for structured shapes.

## Shell Pitfalls (prezto defaults)

The user's shell uses prezto, which sets aliases and options that break non-interactive scripts:

* ❌ `cp src dst` prompts interactively when `dst` exists (prezto aliases `cp` to `cp -i`). Always `rm -f dst` before `cp`. Also kill any process using the destination file first (e.g., `pkill -f winterbaume-server` before replacing the binary).
* ❌ `cat > file <<'EOF'` and `echo > file` fail with `file exists` when the target exists (prezto enables `NO_CLOBBER`). Workaround: `rm -f file` before writing, or use `tee` / `/bin/cat`.
* ❌ `rm file` prompts for confirmation on some files (prezto aliases `rm` to `rm -i`). Always use `rm -f` for non-interactive deletion.
* When running ad-hoc shell scripts that create terraform working directories, always `rm -rf` the entire directory before retrying — stale `.terraform.tfstate.lock.info` files will lock out new runs.

## Git Workflow

* ❌ Neither do `git checkout` nor `git restore`. The other coding agent is concurrently working on the same directory.
* ❌ Never make discretionary commits.

## Documentation

* ❌ For repo-authored documentation only (e.g., `AGENTS.md`, `README.md`, `.agents/docs/**`), never use full-width parentheses (`（` `)`). Instead, use half-width parentheses (`(` `)`) with a half-width space being put before/after an open/close parenthesis when it's preceded/followed by a non-white-space character. This rule does **not** apply to generated or third-party reference files under `skills/**/references/**`.
* ❌ For repo-authored documentation only (e.g., `AGENTS.md`, `README.md`, `.agents/docs/**`), never use full-width colons (`：`). Instead, use a half-width colon followed by a half-width space. This rule does **not** apply to generated or third-party reference files under `skills/**/references/**`.
