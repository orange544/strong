# Source Audit

- Audit date: 2026-04-02
- Scope: Valentine support matrix, local dataset readiness, ISResMat and Unicorn integration feasibility.

## Valentine

- Installed and latest version: `0.4.1`
- Native matchers:
  - `Coma`
  - `SimilarityFlooding`
  - `Cupid`
  - `DistributionBased`
  - `JaccardDistanceMatcher`
- Core metrics callable via `get_metrics`:
  - `Precision`
  - `Recall`
  - `F1Score`
  - `PrecisionTopNPercent`
  - `RecallAtSizeofGroundTruth`
- `MRR` is not built-in and must be calculated in custom evaluator.

## Local benchmark availability snapshot

- `Magellan`: 7 pair tasks (ready)
- `OpenData`: 100 pair tasks (ready)
- `ChEMBL`: no local pair files in current snapshot
- `TPC-DI`: no local pair files in current snapshot
- `Wikidata`: no local pair files in current snapshot

## External baseline status

### ISResMat

- Repository: `duxyad/ISResMat`
- Local clone commit: `7db8498`
- CLI input supports source CSV, target CSV, Valentine mapping JSON.
- Output can include ranked matches when `--store-matches=1`.
- Requires PyTorch/Transformers runtime; cost may be high without GPU.

### Unicorn

- Repository: `ruc-datalab/Unicorn`
- Local clone commit: `5424e58`
- Schema matching is one task in a multi-task framework.
- Input format is pair classification JSON (`[left_serialized, right_serialized, label]`), not raw CSV-pair API.
- Requires checkpoint + candidate serialization pipeline to produce table-column predictions.

## A/B classification

- A (native / near-zero adaptation):
  - COMA
  - SimFlooding
- B (external adaptation required):
  - ISResMat
  - Unicorn

