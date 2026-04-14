# Agent Operating Rules

## Defaults
- Work in lean mode by default.
- Response mode default: `compact` (short + diff + verified).
- Use extended details only on explicit request.

## Scope Gate (Required)
- At the start of each new scope, confirm: `tests`, `README`, `CHANGELOG`, `docstrings`, `response_mode`.
- Keep selected switches fixed until scope completion.
- Reconfirm switches for every new scope.

## Small Scope Policy
- Small scope means: up to 2 files changed and no externally visible behavior/config changes.
- For small scope, use softened flow: implement -> targeted check (or explicit reasoning check if tests are absent) -> report.
- Full verify is required only on explicit request, before release, or when risk is high.

## Documentation Policy
- Update `README` and `CHANGELOG` in batch at the end of a task series.
- Immediate update is required for release-critical or externally visible behavior/config changes.
- Track pending docs updates in `state/docs_todo.md` and keep only open items.

## Language Policy
- Operational docs/process rules: English.
- Code comments/docstrings: Russian.
