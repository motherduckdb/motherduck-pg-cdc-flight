# Agent guide

Instructions for coding agents working in this repository.

## Start here

1. [STATUS.md](STATUS.md) covers completed work, the validation baseline, the
   next implementation steps and the constraints to preserve. Read it first.
2. Read [README.md](README.md) and [DEVELOPING.md](DEVELOPING.md) for setup, test commands and the rules for
   changing code, [dev-docs/design.md](dev-docs/design.md) for the architecture, and the other
   [dev-docs/](dev-docs/) files for the contracts the code relies on.

Setting up a new source → [docs/setup.md](docs/setup.md).

Maintainers keep private working notes (session handoffs, review history) in a separate
repository checked out next to this one as ../motherduck-cdc-flight-notes. It isn't public.
If it's present, read its handoff.md first. Everything needed to build, test and change the
code is in this repository.

## Rules

- **Keep working notes out of tracked files.** Session notes, review requests and
  review findings never go in `docs/` or any other tracked file, and tracked files don't
  link to them.
- **Keep secrets out of the repo.**
  - Credentials live only in the ignored `.env`; `.env.example` is the template.
  - Never print or commit tokens or DSNs.
  - MotherDuck tests use the token from `.env`. Never trigger browser
    authentication.
- **Leave other people's files alone.** Don't delete or modify editor files
  you didn't create, such as `*.swp`.
- **Treat encoding, digests and chunk boundaries as versioned persisted protocols.**
  Read `dev-docs/log-format.md` before changing `format2.py`, `spool2.py`, `pgoutput.py` or
  the cell mapping. Read `dev-docs/apply.md` (the fold rules) before changing
  `sql_fold.py` or the fold in `mirror.py`: both read stored frames, and the
  differential tests hold them equal. Format 2 is frozen by its golden; format 1 is
  historical test evidence. A protocol change needs an approved version/cutover plan and golden
  and recovery evidence, never an edited binding or checkpoint.
- **Keep STATUS.md current.** Update it when a phase finishes or the next steps
  change.
