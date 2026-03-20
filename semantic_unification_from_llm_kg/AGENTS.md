# Repository Guidelines

## Project Structure & Module Organization
- Entry points: `main.py`, plus orchestration modules in `src/pipeline/` (`run.py`, `run_initial.py`, `run_auto.py`, `run_domain_share.py`, `run_sampling.py`).
- Core layers under `src/`:
  - `configs/`: environment and runtime defaults.
  - `db/`: database access and plugin registry (`plugin_registry.py`).
  - `llm/`: description and semantic agents.
  - `service/`: reusable pipeline stage wrappers.
  - `kg/`: Cypher generation.
  - `storage/`: IPFS client and run registry.
- Tests live in `tests/unit/` and `tests/contracts/`.

## Build, Test, and Development Commands
- Install/manage env with `uv` (recommended): `uv sync`.
- Lint: `uv run ruff check src tests`.
- Type check: `uv run mypy --explicit-package-bases src tests`.
- Unit tests with coverage gate:  
  `uv run pytest --cov=src --cov-report=term --cov-fail-under=80 -q`.
- Run pipelines locally:
  - `python main.py --mode sample`
  - `python main.py --mode all`

## Coding Style & Naming Conventions
- Python 3.12+, 4-space indentation, PEP 8 naming.
- Keep strict typing; avoid `Any` unless unavoidable at external boundaries.
- Validate external inputs early (env/IPFS/LLM payloads) and raise explicit `RuntimeError` messages.
- Keep orchestration in `src/pipeline/`; reusable business logic belongs in `src/service/` or lower layers.

## Testing Guidelines
- Add focused unit tests in `tests/unit/test_<module>_guards.py` for edge/error paths.
- Cover at least 3 edge cases per changed flow (invalid shape, empty value, unexpected return type).
- Keep global coverage gate enabled (`--cov-fail-under=80`); current project baseline is much higher, so avoid regressions.

## Commit & Pull Request Guidelines
- Use imperative, scoped commit messages, e.g. `run_auto: validate timestamp token`.
- PRs should include:
  - changed modules and compatibility impact,
  - commands run (ruff/mypy/pytest),
  - env/config changes (`.env` keys, plugin settings).

## Security & Configuration Tips
- Never commit real API keys, chain credentials, or private endpoints.
- For new database support, extend `DatabasePlugin` and register via `DatabasePluginRegistry`; do not hardcode driver logic in pipelines.
- Keep compatibility wrappers (`src/service/*` and pipeline wrapper guards) as stable boundaries; deprecate only after replacement paths are fully tested and documented.
- Treat timestamp/file tokens and external payloads as untrusted input.
