# Project Commands

## Response Format

- Default response format: short summary + diff + what was verified.
- Extended details are provided only on request.

## Scope Gate

- At the start of each new scope, confirm switches: `tests`, `README`, `CHANGELOG`, `docstrings`, `response_mode`.
- `response_mode`: `compact` (short + diff + verified) or `normal` (with extra details).
- Keep these switches fixed until the scope is completed.
- Reconfirm switches for every new scope.

## Documentation Policy

- Update `README`/`CHANGELOG` in batch at the end of a task series, not on every small change.
- Do immediate docs update only for release-critical or externally visible behavior/config changes.

## Common Commands

Run consumer oneshot:

```bash
docker compose run --rm oracle-cdc-consumer-no-validation
```

Run apply oneshot:

```bash
docker compose run --rm oracle-cdc-apply-no-validation
```

Build image:

```bash
docker compose build
```

Targeted checks (preferred by default):

```bash
pytest -q path/to/test_file.py
```

Full checks (only on explicit request or before release/major merge):

```bash
pytest -q
```

- By default, run only targeted tests for the current change scope.
- Run the full test suite only on explicit request or before release/major merge.
