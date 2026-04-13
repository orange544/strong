# Valentine Baseline Experiments

This directory contains reproducible schema-matching baseline experiments centered on the
Valentine benchmark protocol.

## Auto Audit (2026-04-02)

This audit is completed before coding the new experiment module.

### 1) Valentine current support status

- Installed version: `valentine==0.4.1`
- PyPI latest version check: `0.4.1`
- Python requirement from Valentine project metadata: `>=3.10,<3.15`
- Java requirement for COMA: confirmed (`java 17` available in this environment)

### 2) Matchers currently supported natively by Valentine

From `valentine/algorithms/__init__.py` in the Valentine source:

- Schema+Instance: `Coma`
- Schema-only: `SimilarityFlooding`, `Cupid`
- Instance-only: `DistributionBased`, `JaccardDistanceMatcher`

Conclusion for this experiment:

- `COMA` and `SimFlooding` are native and should use Valentine interfaces first.
- `ISResMat` and `Unicorn` are not native Valentine matchers and must be integrated via external adapters.

### 3) Metrics callable from Valentine `matches.get_metrics(...)`

From `valentine/metrics` source (`METRICS_CORE`):

- `Precision`
- `Recall`
- `F1Score`
- `PrecisionTopNPercent` (default top-10%)
- `RecallAtSizeofGroundTruth`

Important:

- `MRR` is **not** a built-in Valentine metric and must be computed by our unified evaluator only when
  ranked predictions with meaningful scores are available.

### 4) Which datasets are currently suitable for unified comparison

Local benchmark root:
`program/database/valentine_experiments/data/valentine_datasets_magellan_20260401/Valentine-datasets`

Observed local availability:

- `Magellan`: 7 pair tasks with `*_source.csv`, `*_target.csv`, `*_mapping.json`
- `OpenData`: 100 pair tasks with compatible files
- `ChEMBL`: present as folder but no local pair files in current snapshot
- `TPC-DI`: present as folder but no local pair files in current snapshot
- `Wikidata`: present as folder but no local pair files in current snapshot

Most stable comparison set in current environment:

1. Stage-1: `Magellan` (small, fast sanity and reproducibility)
2. Stage-2: `OpenData` subset then full

### 5) ISResMat open-source availability and execution conditions

- Repo: `https://github.com/duxyad/ISResMat` (cloned locally under `external/isresmat_repo`)
- Commit checked: `7db8498`
- Environment hint from upstream: conda `python=3.9`, `pytorch`, `pytorch-cuda=11.7`, `transformers`
- Input format:
  - `--orig-file-src` -> source CSV
  - `--orig-file-tgt` -> target CSV
  - `--orig-file-golden-matches` -> Valentine-style mapping JSON
  - `--dataset-name` -> unique pair id
- Output format:
  - JSON files under `data/output/<comment>/<dataset>.json`
  - includes metrics and (optionally) ranked matches when `--store-matches=1`
- Running condition:
  - practical runtime depends on PyTorch/Transformers availability; may be slow without GPU.

### 6) Unicorn open-source availability and execution conditions

- Repo: `https://github.com/ruc-datalab/Unicorn` (cloned locally under `external/unicorn_repo`)
- Commit checked: `5424e58`
- Upstream environment:
  - README states Python `3.6.5`
  - `torch==1.7.1` (README) / `torch==1.8.0` in `requirements.txt`
  - transformer-based encoder, CUDA-oriented setup in upstream examples
- Input format:
  - JSON list samples: `[serialized_left, serialized_right, label]`
  - Schema matching task is one task among multi-task datasets (`fabricated_dataset`, `DeepMDatasets`)
- Output format:
  - evaluation script prints aggregate `F1/Recall/ACC`
  - optional `prob.json` by sample id (if `flag=get_prob`)
- Running condition:
  - not a direct CSV-to-schema-matching API; requires task-specific dataset preparation and model checkpoint
    (pretrain/fine-tune or upstream HF weights).

## Stable implementation strategy

1. Build a unified experiment module under `experiments/valentine_baselines`.
2. Use Valentine native adapters for `COMA` and `SimFlooding`.
3. Use external adapters for `ISResMat` and `Unicorn`:
   - first support robust availability checks + subprocess integration;
   - if runtime prerequisites are missing, mark `NOT_AVAILABLE` with explicit reason (no fake scores).
4. Use one evaluator for all methods:
   - always: `Precision`, `Recall`, `F1`
   - optional: `MRR`, `Recall@GT` only when ranked predictions are present.
5. Produce paper-ready outputs:
   - per-pair CSV, method summary CSV, markdown report, and LaTeX table.

## Legacy scripts

The following scripts remain usable for quick checks:

- `run_valentine_baselines.py`
- `run_magellan_baselines.py`
