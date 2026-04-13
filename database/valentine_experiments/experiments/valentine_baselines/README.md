# Valentine Baseline Comparison Module

This module provides a reproducible pipeline for baseline comparison on Valentine benchmark
protocols.

## Implemented Methods

- `coma` (Valentine native)
- `simflooding` (Valentine native)
- `isresmat` (external adapter)
- `unicorn` (external adapter)
- `ours_sda` (custom Sample-Describe-Aggregate adapter)
  - description stage defaults to `llm` mode (OpenAI-compatible API)
  - config file: `configs/ours_sda.yaml`
  - env keys follow main architecture: `LLM_DESC_*` with `LLM_*` fallback
- `ours_arch` (main architecture path: sampling -> LLM description -> aggregation)
  - runs by converting each Valentine pair CSV into temporary SQLite databases
  - then calls modules from `semantic_unification_from_llm_kg` without changing main architecture code
  - config file: `configs/ours_arch.yaml`

## Unified Evaluation

All methods are normalized to:

```json
[
  {
    "source_table": "...",
    "source_column": "...",
    "target_table": "...",
    "target_column": "...",
    "score": 0.0
  }
]
```

Metrics:

- Always: `Precision`, `Recall`, `F1`
- Optional (only when ranked scores are available): `MRR`, `Recall@GT`

If a method has no ranked output, `MRR` is left empty.

## Quick Run

Run all configured methods and datasets:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_all.py"
```

Run only the custom SDA method on all configured datasets:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_ours_all_datasets.py"
```

Run baseline methods plus custom SDA in one unified pass:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_compare_plus_ours.py"
```

Run main-architecture method for one pair:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_ours_arch_single_pair.py" --source "<source.csv>" --target "<target.csv>" --ground-truth "<mapping.json>"
```

Run main-architecture method for one dataset:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_ours_arch_dataset.py" --dataset Magellan
```

Run main-architecture method for all configured datasets:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_ours_arch_all.py"
```

Run one method on one dataset:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_dataset_batch.py" --method coma --dataset Magellan
```

Run one single pair:

```powershell
python "D:\Program Files\BISHE\program\database\valentine_experiments\experiments\valentine_baselines\runners\run_single_pair.py" --method simflooding --source "...\_source.csv" --target "...\_target.csv" --ground-truth "...\_mapping.json"
```

## Outputs

- Raw predictions: `outputs/raw_predictions/{method}/{dataset}/{pair}.json`
- Per-pair metrics: `outputs/metrics/per_pair/*.csv`
- Main summary: `outputs/metrics/summary_all_methods.csv`
- Markdown table: `outputs/metrics/summary_all_methods.md`
- LaTeX table: `outputs/metrics/summary_all_methods.tex`
- Figures: `outputs/figures/f1_by_method.png`, `runtime_by_method.png`, optional `mrr_by_method.png`
- Experiment report: `reports/experiment_report.md`

## Notes On External Methods

- `ISResMat` and `Unicorn` are non-native Valentine methods.
- Default configuration keeps them disabled to avoid fake/incomplete results.
- Enable them only when dependencies/checkpoints are ready.
- If unavailable, pipeline records `NOT_AVAILABLE` without breaking the full run.
