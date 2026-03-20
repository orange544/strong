# Golden Baseline Guide

## Purpose
Golden artifacts represent accepted baseline behavior before refactor.  
Contract tests compare current outputs against normalized golden outputs.

## Scenarios
1. `sample`: `python main.py --mode sample`
2. `all`: `python main.py --mode all`
3. `domain_share_debug`: `python run_domain_share.py --mock-llm --skip-chain --max-fields-per-domain 10`

## Directory Convention
Use this layout when capturing fixtures:

```text
tests/golden/
  normalize_rules.json
  sample/
    raw/
    normalized/
  all/
    raw/
    normalized/
  domain_share_debug/
    raw/
    normalized/
```

## Capture Procedure
1. Run baseline scripts under `scripts/baseline/` to produce sanitized logs.
2. Copy generated JSON artifacts from `outputs/` into `tests/golden/<scenario>/raw/`.
3. Copy the corresponding sanitized log from `outputs/baseline_logs/`.
4. Normalize artifacts according to `tests/golden/normalize_rules.json`.
5. Save normalized files into `tests/golden/<scenario>/normalized/`.

## What Must Be Stable
1. JSON schema and required key presence.
2. Core semantic fields (`table`, `field`, `description`, canonical grouping structure).
3. Registry append structure (`runs[]` entry shape).

## What Is Allowed To Change
1. `timestamp` values.
2. CIDs and transaction hashes.
3. Absolute local paths.
4. Order of parallel-generated description records (must be normalized by stable sort key).

## Security Rules
1. Never store raw logs with secrets in `tests/golden/`.
2. Keep only sanitized logs.
3. Do not commit real API keys, receiver addresses, or tokens.
