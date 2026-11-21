# Contributing

This is an experimental release. MotherDuck doesn't provide official support for it.

**This repository doesn't accept issues or pull requests.** To use or change the code, fork it.
Dependency updates may arrive as Dependabot pull requests. Report security problems privately,
as [SECURITY.md](SECURITY.md) describes.

## Working on a fork

- [DEVELOPING.md](DEVELOPING.md) has the setup, the test commands and the rules for
  changing code. [dev-docs/](dev-docs/) holds the contracts the code relies on.

## Tests

- `uv sync --dev`, then `uv run pytest -q` runs the local checks. The MotherDuck cases skip
  without a MotherDuck token, so you don't need a MotherDuck account to run them.
- The PostgreSQL tests need the local database from `compose.yaml` (DEVELOPING.md, "Tests
  and CI").
- CI runs the local and PostgreSQL tests on every push. It uses no secrets.
- A Dive change also runs `npm test` in `dive/`.

## Rules that matter most

- **The persisted formats are frozen.** Format 2 (`dev-docs/log-format.md`) and the fold rules
  (`dev-docs/apply.md`) are versioned protocols, pinned by `tests/test_docs.py` and the
  golden fixtures. A change to them needs a versioned cutover, not an edit.
- **Keep secrets out of the repository.** Credentials go in the git-ignored `.env`
  (`.env.example` is the template). Never commit or paste tokens or DSNs.
- **Keep internal references out of tracked files.** `tests/test_docs.py` checks for them.

## License

There's no CLA or DCO. By contributing, you agree that your contribution is licensed under
the Apache License, Version 2.0, the same license as the project ([LICENSE](LICENSE)).
