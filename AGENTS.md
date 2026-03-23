# Repository Guidelines

## Project Structure & Module Organization
- `blockchain/go-norn-main/`: Go blockchain node and IPFS-chain bridge. Entry binaries live in `cmd/`; core logic is in `core/`, `p2p/`, `rpc/`, `pubsub/`, `crypto/`, and `utils/`.
- `semantic_unification_from_llm_kg/`: Python semantic-unification pipeline. Main code is in `src/`; tests are in `tests/unit`, `tests/contracts`, and `tests/golden`; helper scripts are in `scripts/baseline/`.
- `database/movie/`: Multi-engine movie schema/data initialization scripts. Each engine has its own folder and `apply.ps1`; shared setup runs from `setup_movie_databases.ps1`.

## Build, Test, and Development Commands
- Go (`blockchain/go-norn-main`):
  - `go build ./cmd/norn` builds the node binary.
  - `go build -o ./bin/ipfs-chain ./cmd/ipfs-chain` builds the bridge CLI.
  - `go test ./...` runs all Go tests.
- Python (`semantic_unification_from_llm_kg`):
  - `uv sync` installs runtime and dev dependencies.
  - `uv run ruff check src tests` runs lint checks.
  - `uv run mypy --explicit-package-bases src tests` runs strict type checks.
  - `uv run pytest --cov=src --cov-fail-under=80 -q` runs tests with coverage gating.
  - `python main.py --mode sample` runs a local sample pipeline.
- Database scripts (`database/movie`):
  - `.\setup_movie_databases.ps1` initializes supported local databases.

## Coding Style & Naming Conventions
- Go: format with `gofmt`, keep package names lowercase, and place tests beside code using `_test.go`.
- Python: 4-space indentation, type hints by default, max line length 100, and Ruff/MyPy compliance before PR.
- Keep file naming engine-specific and explicit (for example, `movie_postgresql.sql`, `movie_neo4j.cypher`, `apply.ps1`).

## Testing Guidelines
- Go: add tests in the touched package and run `go test ./...` before submission.
- Python: add tests under `tests/unit` or `tests/contracts` with names like `test_<module>_*.py`.
- Preserve Python coverage threshold (`--cov-fail-under=80`).
- For database changes, run the relevant `apply.ps1` or full setup and validate created schema/data.

## Commit & Pull Request Guidelines
- Follow the existing commit pattern: `scope: imperative summary` (for example, `pipeline: harden runtime guards` or `db,kg: stabilize plugin registry`).
- Keep commits focused by subsystem and avoid mixing unrelated changes.
- PRs should include changed paths, commands run (lint/type/test), config or credential impact, and sample CLI output when behavior changes.

## Security & Configuration Tips
- Never commit secrets; keep real values in local `.env` files only.
- Avoid committing generated outputs/logs/databases and local binaries.
- Treat external inputs (LLM/IPFS/DB responses) as untrusted and validate at integration boundaries.
