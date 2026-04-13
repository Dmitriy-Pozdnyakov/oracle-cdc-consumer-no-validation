# Coding Standards

- Prefer small, focused changes.
- Keep comments concise and useful.
- Validate inputs early.
- Fail fast on misconfiguration.
- Keep README and changelog in sync with behavior.
- Write function docstrings/descriptions in Russian.
- Update README when behavior, config, or run commands change.
- Keep `state/runtime_flow.md` in sync with real runtime pipeline (`consumer.py` + `apply.py`).
- Do not add runtime auto-generation of architecture docs into service code unless explicitly requested.
- For major flow updates, start from template `.codex/templates/runtime_flow.md.tpl`.
