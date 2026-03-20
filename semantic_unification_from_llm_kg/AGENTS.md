# Repository Guidelines

## Project Structure & Module Organization
- `main.py` is the primary CLI entry (`--mode sample|all`).
- `run_domain_share.py` runs the per-domain IPFS/chain workflow.
- Core code lives in `src/`:
  - `configs/` environment loading and runtime defaults.
  - `db/` SQLite sampling and schema extraction.
  - `llm/` field description and semantic unification agents.
  - `pipeline/` orchestration (`run.py`, `run_sampling.py`, `run_domain_share.py`, `run_auto.py`).
  - `service/` reusable pipeline steps.
  - `storage/` IPFS client and run registry.
  - `kg/` Cypher generation helpers.
- Data and outputs:
  - SQLite inputs: `data/dbs/*.db`
  - Generated artifacts/logs: `outputs/`
  - Run index: `ipfs_registry.json`

## Build, Test, and Development Commands
- Create environment and install minimal dependencies:
  - `python -m venv .venv`
  - `.venv\Scripts\activate`
  - `pip install openai requests`
- Run sampling only: `python main.py --mode sample`
- Run sampling and upload to IPFS: `python main.py --mode sample --upload-ipfs`
- Run full pipeline: `python main.py --mode all`
- Run domain-share debug flow: `python run_domain_share.py --domain IMDB --mock-llm --skip-chain --max-fields-per-domain 10`
- Quick syntax smoke test: `python -m compileall src main.py run_domain_share.py`

## Coding Style & Naming Conventions
- Target Python 3.12+ style with 4-space indentation and PEP 8 naming.
- Use `snake_case` for functions/files/variables and `PascalCase` for classes.
- Keep type hints on public functions and return structured `dict` artifacts for pipeline stages.
- Keep orchestration in `src/pipeline/` and reusable logic in `src/service/` or lower-level modules.
- Prefer config from `.env` (`src/configs/config.py`) over hard-coded paths or credentials.

## Testing Guidelines
- There is no dedicated `tests/` suite in this snapshot; use script-level smoke tests before PRs.
- Minimum validation for changes:
  - run the relevant CLI command(s),
  - confirm new JSON artifacts are written under `outputs/`,
  - verify no traceback in logs.
- For new logic-heavy code, add `pytest` tests under `tests/` named `test_<module>.py`.

## Commit & Pull Request Guidelines
- Git metadata is not present in this workspace, so historical commit conventions cannot be verified here.
- Use short, imperative, scoped commit messages (example: `pipeline: add domain filter guard`).
- PRs should include:
  - what changed and why,
  - related issue/task ID,
  - commands run for validation,
  - notable `.env` keys or runtime prerequisites.

## Security & Configuration Tips
- Do not commit real API keys, RPC endpoints, or private chain credentials.
- Keep secrets in `.env`; rotate keys if logs accidentally expose them.
- Avoid committing large generated logs/artifacts unless required for reproducibility.
