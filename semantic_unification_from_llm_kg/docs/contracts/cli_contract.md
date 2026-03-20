# CLI Behavior Contract (T0 Baseline)

## Scope
This contract defines baseline behavior for current CLI entrypoints before refactor.  
Refactor changes must preserve this behavior unless explicitly listed as "Allowed Drift".

## Entrypoints
1. `python main.py --mode sample [--upload-ipfs]`
2. `python main.py --mode all`
3. `python run_domain_share.py [--domain <name>] [--max-fields-per-domain N] [--skip-chain] [--mock-llm] [--strict]`

## Output Contract
### `sample` mode
1. Writes `outputs/samples_<timestamp>.json`.
2. Prints a summary dict with keys:
   `timestamp`, `output_file`, `total_fields`, `databases`.
3. If `--upload-ipfs` is enabled, summary includes `samples_cid`.

Sample artifact item schema:
- `table: str`
- `field: str`
- `type: str`
- `samples: list`
- `db_name: str`

### `all` mode
For each domain, writes:
1. `outputs/samples_<db_tag>_<timestamp>.json` (object with `summary` and `samples`)
2. `outputs/field_descriptions_<db_tag>_<timestamp>.json` (object with `summary` and `field_descriptions`)
3. `outputs/domain_unified_<db_tag>_<timestamp>.json`

Global outputs:
1. `outputs/unified_fields_<timestamp>.json`
2. `outputs/cypher_<timestamp>.json`
3. Appends one record to `ipfs_registry.json` (`runs[]`)

### `run_domain_share.py`
Per selected domain, writes:
1. `outputs/samples_<domain_slug>_<timestamp>.json` (list)
2. `outputs/field_descriptions_<domain_slug>_<timestamp>.json` (list)
3. `outputs/domain_share_manifest_<timestamp>.json` (object)
4. Appends one record to `ipfs_registry.json` (`runs[]`)

## Failure Contract
1. Empty `DB_PATHS` fails with `RuntimeError`.
2. `--mode all` requires usable IPFS + chain path; build/put failures are hard failures.
3. `run_domain_share.py`:
   - default: per-domain errors are captured in manifest (`status=failed`) and loop continues.
   - with `--strict`: first domain failure aborts run with exception.
4. Invalid `--domain` selection fails with explicit available-domain message.

## Allowed Drift (Normalization Required)
1. Timestamp values and timestamp-derived filenames.
2. CID and TxHash values.
3. Absolute local paths (`output_file`, `samples_file`, `field_descriptions_file`, `db_path`).
4. Log ordering caused by parallel LLM description generation.

## Forbidden Drift
1. CLI flags/semantics above.
2. Required top-level keys in summary/manifest artifacts.
3. Registry append behavior (`ipfs_registry.json` must still append a run record).
4. Artifact JSON must remain UTF-8 and valid JSON.

## Baseline Capture
Use baseline scripts:
1. `scripts/baseline/run_sample.ps1`
2. `scripts/baseline/run_all.ps1`
3. `scripts/baseline/run_domain_share.ps1`

Each script writes sanitized logs to `outputs/baseline_logs/`. Raw logs are removed after redaction.
