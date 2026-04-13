# Experiment Report

- Run timestamp: `20260402_213652`
- Methods: `coma, simflooding, isresmat, unicorn`
- Datasets: `Magellan, OpenData`
- Total pair runs: `428`

## Reproduction Status

- not_available: 214
- ok: 214

## Main Results

| Method | Dataset | P | R | F1 | Runtime(s) | Status | Notes |
|---|---|---:|---:|---:|---:|---|---|
| coma | Magellan | 1.0000 | 1.0000 | 1.0000 | 0.74 | ok |  |
| coma | OpenData | 0.6610 | 0.6346 | 0.5457 | 1.24 | ok |  |
| isresmat | Magellan |  |  |  |  | failed | ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.; ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.; ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime. |
| isresmat | OpenData |  |  |  |  | failed | ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.; ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.; ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime. |
| simflooding | Magellan | 1.0000 | 1.0000 | 1.0000 | 0.07 | ok |  |
| simflooding | OpenData | 0.3534 | 0.7441 | 0.4321 | 3.26 | ok |  |
| unicorn | Magellan |  |  |  |  | failed | Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.; Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.; Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime. |
| unicorn | OpenData |  |  |  |  | failed | Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.; Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.; Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime. |

## Failed / Not Available Items

- `isresmat` / `Magellan` / `amazon_google_exp` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `Magellan` / `beeradvo_ratebeer` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `Magellan` / `dblp_acm` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `Magellan` / `dblp_scholar` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `Magellan` / `fodors_zagats` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `Magellan` / `itunes_amazon` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `Magellan` / `walmart_amazon` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `unicorn` / `Magellan` / `amazon_google_exp` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `unicorn` / `Magellan` / `beeradvo_ratebeer` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `unicorn` / `Magellan` / `dblp_acm` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `unicorn` / `Magellan` / `dblp_scholar` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `unicorn` / `Magellan` / `fodors_zagats` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `unicorn` / `Magellan` / `itunes_amazon` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `unicorn` / `Magellan` / `walmart_amazon` -> `not_available`: Unicorn adapter is disabled. Provide prediction_file or set enabled=true with a prepared external runtime.
- `isresmat` / `OpenData` / `miller2_both_50_1_ac1_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_1_ac2_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_1_ac3_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_1_ac4_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_1_ac5_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_1_ec_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_30_ac1_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_30_ac2_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_30_ac3_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_30_ac4_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_30_ac5_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_30_ec_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_50_ac1_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_50_ac2_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_50_ac3_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_50_ac4_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_50_ac5_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_50_ec_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_70_ac1_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_70_ac2_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_70_ac3_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_70_ac4_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_70_ac5_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_both_50_70_ec_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_1_ac1_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_1_ac2_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_1_ac3_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_1_ac4_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_1_ac5_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_1_ec_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_30_ac1_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_30_ac2_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_30_ac3_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_30_ac4_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_30_ac5_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.
- `isresmat` / `OpenData` / `miller2_vertical_30_ec_ev` -> `not_available`: ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime.

## Fairness And Limitations

- COMA and SimFlooding use native Valentine interfaces.
- ISResMat and Unicorn are integrated through external adapters.
- If a method does not provide ranked scores, MRR is not computed.
- Any unavailable method is marked explicitly; no metric is fabricated.
