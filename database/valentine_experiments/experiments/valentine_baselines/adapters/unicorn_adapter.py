from __future__ import annotations

import csv
import importlib
import re
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import MethodPrediction, PairSample, prediction_row
from ..utils.io import read_json


@dataclass(frozen=True)
class _RuntimeKey:
    repo_path: str
    model: str
    ckpt: str
    checkpoint_dir: str
    wmoe: bool
    expertsnum: int
    units: int
    size_output: int
    load_balance: bool
    batch_size: int
    max_seq_length: int


@dataclass
class _Runtime:
    key: _RuntimeKey
    device: Any
    tokenizer: Any
    encoder: Any
    moelayer: Any | None
    classifier: Any
    torch: Any
    batch_size: int
    max_seq_length: int
    wmoe: bool


_RUNTIME_CACHE: _Runtime | None = None


def _normalize_text(text: str, *, max_chars: int) -> str:
    value = re.sub(r"\s+", " ", str(text)).strip()
    if not value:
        return ""
    if len(value) > max_chars:
        return value[:max_chars]
    return value


def _open_csv_with_fallback(path: Path) -> tuple[Any, list[str], Any]:
    errors: list[str] = []
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        handle = None
        try:
            handle = path.open("r", encoding=encoding, newline="")
            reader = csv.reader(handle)
            header = next(reader, None)
            if header is None:
                handle.close()
                return None, [], None
            return handle, [str(item) for item in header], reader
        except UnicodeDecodeError as exc:
            errors.append(f"{encoding}: {exc}")
            if handle is not None:
                handle.close()
    raise ValueError(f"Unable to decode CSV {path}: {'; '.join(errors)}")


def _serialize_columns(
    path: Path,
    *,
    max_values_per_column: int,
    max_value_chars: int,
) -> list[tuple[str, str]]:
    handle, headers, reader = _open_csv_with_fallback(path)
    if handle is None or reader is None or not headers:
        return []

    sample_values: list[list[str]] = [[] for _ in headers]
    pending = set(range(len(headers)))
    try:
        for row in reader:
            if not pending:
                break
            for idx in list(pending):
                raw = row[idx] if idx < len(row) else ""
                normalized = _normalize_text(raw, max_chars=max_value_chars)
                if not normalized:
                    continue
                sample_values[idx].append(normalized)
                if len(sample_values[idx]) >= max_values_per_column:
                    pending.remove(idx)
    finally:
        handle.close()

    serialized: list[tuple[str, str]] = []
    for idx, raw_name in enumerate(headers):
        col_name = _normalize_text(raw_name, max_chars=max_value_chars) or f"column_{idx + 1}"
        values = sample_values[idx] if sample_values[idx] else ["NULL"]
        payload = f"[ATT] {col_name} " + " ".join(f"[VAL] {value}" for value in values)
        serialized.append((col_name, payload))
    return serialized


def _torch_load_state(module: Any, *, path: Path, torch_mod: Any, device: Any) -> None:
    state = torch_mod.load(str(path), map_location=device)
    if isinstance(state, dict) and "state_dict" in state and isinstance(state["state_dict"], dict):
        state = state["state_dict"]
    if isinstance(state, dict) and state and all(str(k).startswith("module.") for k in state.keys()):
        state = {str(k)[7:]: v for k, v in state.items()}
    incompatible = module.load_state_dict(state, strict=False)
    missing_keys = list(incompatible.missing_keys)
    unexpected_keys = [key for key in incompatible.unexpected_keys if key != "encoder.embeddings.position_ids"]
    if missing_keys or unexpected_keys:
        raise RuntimeError(
            f"state_dict mismatch for {path.name}: missing={missing_keys}, unexpected={unexpected_keys}"
        )


def _build_runtime(method_cfg: dict[str, Any]) -> _Runtime:
    repo_path = Path(str(method_cfg.get("repo_path", "")))
    if not repo_path.exists():
        raise FileNotFoundError(f"Unicorn repo path not found: {repo_path}")

    model = str(method_cfg.get("model", "deberta_base")).strip()
    if model != "deberta_base":
        raise ValueError(
            f"Unsupported Unicorn model '{model}'. Only 'deberta_base' is supported in this adapter."
        )
    ckpt = str(method_cfg.get("ckpt", "UnicornPlus")).strip() or "UnicornPlus"
    checkpoint_dir = Path(str(method_cfg.get("checkpoint_dir", (repo_path / "checkpoint").as_posix())))
    batch_size = int(method_cfg.get("batch_size", 32))
    max_seq_length = int(method_cfg.get("max_seq_length", 128))
    wmoe = bool(method_cfg.get("wmoe", True))
    expertsnum = int(method_cfg.get("expertsnum", 6))
    units = int(method_cfg.get("units", 768))
    size_output = int(method_cfg.get("size_output", 768))
    load_balance = bool(method_cfg.get("load_balance", False))

    key = _RuntimeKey(
        repo_path=str(repo_path.resolve()),
        model=model,
        ckpt=ckpt,
        checkpoint_dir=str(checkpoint_dir.resolve()),
        wmoe=wmoe,
        expertsnum=expertsnum,
        units=units,
        size_output=size_output,
        load_balance=load_balance,
        batch_size=batch_size,
        max_seq_length=max_seq_length,
    )

    global _RUNTIME_CACHE
    if _RUNTIME_CACHE is not None and _RUNTIME_CACHE.key == key:
        return _RUNTIME_CACHE

    repo_sys_path = str(repo_path.resolve())
    if repo_sys_path not in sys.path:
        sys.path.insert(0, repo_sys_path)

    torch_mod = importlib.import_module("torch")
    device = torch_mod.device("cuda" if torch_mod.cuda.is_available() else "cpu")

    transformers_mod = importlib.import_module("transformers")
    DebertaModel = getattr(transformers_mod, "DebertaModel")
    DebertaTokenizer = getattr(transformers_mod, "DebertaTokenizer")
    try:
        hf_logging = importlib.import_module("transformers.utils.logging")
        hf_logging.set_verbosity_error()
    except Exception:
        pass

    matcher_mod = importlib.import_module("unicorn.model.matcher")
    moe_mod = importlib.import_module("unicorn.model.moe")

    class _DebertaBaseEncoder(torch_mod.nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.encoder = DebertaModel.from_pretrained("microsoft/deberta-base")

        def forward(self, x: Any, mask: Any | None = None, segment: Any | None = None) -> Any:
            outputs = self.encoder(x, attention_mask=mask, token_type_ids=segment)
            return outputs.last_hidden_state[:, 0, :]

    tokenizer = DebertaTokenizer.from_pretrained("microsoft/deberta-base")
    encoder = _DebertaBaseEncoder()
    classifier = getattr(matcher_mod, "MOEClassifier")(units) if wmoe else getattr(matcher_mod, "Classifier")()
    moelayer = (
        getattr(moe_mod, "MoEModule")(size_output, units, expertsnum, load_balance=load_balance)
        if wmoe
        else None
    )

    encoder_ckpt = checkpoint_dir / f"{ckpt}_encoder.pt"
    cls_ckpt = checkpoint_dir / f"{ckpt}_cls.pt"
    moe_ckpt = checkpoint_dir / f"{ckpt}_moe.pt"
    for ckpt_path in (encoder_ckpt, cls_ckpt):
        if not ckpt_path.exists():
            raise FileNotFoundError(f"Unicorn checkpoint not found: {ckpt_path}")
    if wmoe and not moe_ckpt.exists():
        raise FileNotFoundError(f"Unicorn checkpoint not found: {moe_ckpt}")

    _torch_load_state(encoder, path=encoder_ckpt, torch_mod=torch_mod, device=device)
    _torch_load_state(classifier, path=cls_ckpt, torch_mod=torch_mod, device=device)
    if wmoe and moelayer is not None:
        _torch_load_state(moelayer, path=moe_ckpt, torch_mod=torch_mod, device=device)

    encoder = encoder.to(device).eval()
    classifier = classifier.to(device).eval()
    if moelayer is not None:
        moelayer = moelayer.to(device).eval()

    runtime = _Runtime(
        key=key,
        device=device,
        tokenizer=tokenizer,
        encoder=encoder,
        moelayer=moelayer,
        classifier=classifier,
        torch=torch_mod,
        batch_size=max(1, batch_size),
        max_seq_length=max(8, max_seq_length),
        wmoe=wmoe,
    )
    _RUNTIME_CACHE = runtime
    return runtime


def _infer_candidate_scores(
    *,
    runtime: _Runtime,
    source_columns: list[tuple[str, str]],
    target_columns: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    candidates: list[tuple[str, str]] = []
    left_texts: list[str] = []
    right_texts: list[str] = []
    for src_name, src_text in source_columns:
        for tgt_name, tgt_text in target_columns:
            candidates.append((src_name, tgt_name))
            left_texts.append(src_text)
            right_texts.append(tgt_text)

    total = len(candidates)
    if total == 0:
        return []

    scores: list[float] = [0.0] * total
    torch_mod = runtime.torch
    with torch_mod.inference_mode():
        for begin in range(0, total, runtime.batch_size):
            end = min(total, begin + runtime.batch_size)
            encoded = runtime.tokenizer(
                left_texts[begin:end],
                right_texts[begin:end],
                padding=True,
                truncation=True,
                max_length=runtime.max_seq_length,
                return_tensors="pt",
                verbose=False,
            )
            input_ids = encoded["input_ids"].to(runtime.device)
            attention_mask = encoded["attention_mask"].to(runtime.device)
            token_type_ids = encoded.get("token_type_ids")
            if token_type_ids is not None:
                token_type_ids = token_type_ids.to(runtime.device)

            feat = runtime.encoder(input_ids, attention_mask, token_type_ids)
            if runtime.wmoe:
                moe_out = runtime.moelayer(feat) if runtime.moelayer is not None else feat
                moe_feat = moe_out[0] if isinstance(moe_out, tuple) else moe_out
                logits = runtime.classifier(moe_feat)
            else:
                logits = runtime.classifier(feat)

            if len(logits.shape) != 2 or logits.shape[1] < 2:
                raise ValueError(f"Unexpected Unicorn logits shape: {tuple(logits.shape)}")
            batch_scores = logits[:, 1].detach().cpu().tolist()
            scores[begin:end] = [float(value) for value in batch_scores]

    rows: list[dict[str, Any]] = []
    for idx, (src_name, tgt_name) in enumerate(candidates):
        rows.append(prediction_row("__source__", src_name, "__target__", tgt_name, scores[idx]))
    rows.sort(key=lambda item: item["score"], reverse=True)
    return rows


def _prediction_file_mode(pair: PairSample, prediction_file: Path, supports_ranking: bool) -> MethodPrediction:
    payload = read_json(prediction_file)
    if not isinstance(payload, list):
        raise ValueError("prediction_file must be a JSON list")

    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        rows.append(
            prediction_row(
                str(item["source_table"]),
                str(item["source_column"]),
                str(item["target_table"]),
                str(item["target_column"]),
                float(item.get("score", 1.0)),
            )
        )
    rows.sort(key=lambda x: x["score"], reverse=True)
    return MethodPrediction(
        method="unicorn",
        status="ok",
        predictions=rows,
        supports_ranking=supports_ranking,
        metadata={
            "mode": "prediction_file",
            "prediction_file": str(prediction_file),
            "pair_id": pair.pair_id,
        },
    )


def run(pair: PairSample, method_cfg: dict[str, Any]) -> MethodPrediction:
    start = time.perf_counter()
    try:
        supports_ranking = bool(method_cfg.get("supports_ranking", True))
        prediction_file = str(method_cfg.get("prediction_file", "")).strip()
        if prediction_file:
            pred = _prediction_file_mode(pair, Path(prediction_file), supports_ranking)
            pred.runtime_sec = time.perf_counter() - start
            pred.inference_sec = pred.runtime_sec
            return pred

        enabled = bool(method_cfg.get("enabled", False))
        if not enabled:
            return MethodPrediction(
                method="unicorn",
                status="not_available",
                runtime_sec=time.perf_counter() - start,
                error_message=(
                    "Unicorn adapter is disabled. Provide prediction_file or set enabled=true with "
                    "a prepared external runtime."
                ),
            )

        prepare_start = time.perf_counter()
        runtime = _build_runtime(method_cfg)
        max_values_per_column = int(method_cfg.get("max_values_per_column", 20))
        max_value_chars = int(method_cfg.get("max_value_chars", 80))
        source_columns = _serialize_columns(
            pair.source_path,
            max_values_per_column=max(1, max_values_per_column),
            max_value_chars=max(16, max_value_chars),
        )
        target_columns = _serialize_columns(
            pair.target_path,
            max_values_per_column=max(1, max_values_per_column),
            max_value_chars=max(16, max_value_chars),
        )
        prepare_sec = time.perf_counter() - prepare_start

        if not source_columns or not target_columns:
            return MethodPrediction(
                method="unicorn",
                status="error",
                runtime_sec=time.perf_counter() - start,
                data_loading_sec=prepare_sec,
                error_message="Failed to extract source/target columns for Unicorn inference.",
            )

        infer_start = time.perf_counter()
        raw_rows = _infer_candidate_scores(
            runtime=runtime,
            source_columns=source_columns,
            target_columns=target_columns,
        )
        infer_sec = time.perf_counter() - infer_start

        rows = [
            prediction_row(
                pair.source_table_name,
                str(item["source_column"]),
                pair.target_table_name,
                str(item["target_column"]),
                float(item["score"]),
            )
            for item in raw_rows
        ]

        return MethodPrediction(
            method="unicorn",
            status="ok",
            predictions=rows,
            supports_ranking=supports_ranking,
            runtime_sec=time.perf_counter() - start,
            data_loading_sec=prepare_sec,
            inference_sec=infer_sec,
            metadata={
                "repo_path": str(method_cfg.get("repo_path", "")),
                "model": str(method_cfg.get("model", "deberta_base")),
                "ckpt": str(method_cfg.get("ckpt", "UnicornPlus")),
                "device": str(runtime.device),
                "source_columns": len(source_columns),
                "target_columns": len(target_columns),
                "candidate_pairs": len(rows),
            },
        )
    except Exception as exc:  # pragma: no cover - runtime dependent
        return MethodPrediction(
            method="unicorn",
            status="error",
            runtime_sec=time.perf_counter() - start,
            error_message=f"{type(exc).__name__}: {exc}",
            metadata={"traceback": traceback.format_exc()},
        )
