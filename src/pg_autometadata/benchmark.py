import argparse
import copy
import csv
import json
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pg_autometadata.pipeline import (
    ensure_parent,
    heuristic_infer,
    load_structured_file,
    openai_compatible_infer,
    read_jsonl,
    write_jsonl,
)


def slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    text = text.strip("-")
    return text or "model"


def select_records(records: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    ev = cfg.get("evaluation", {})
    limit = int(ev.get("sample_limit", 100))
    method = str(ev.get("selection_method", "random")).lower()
    seed = int(ev.get("random_seed", 42))

    if limit <= 0 or limit >= len(records):
        return records

    if method == "first":
        return records[:limit]

    rng = random.Random(seed)
    idx = list(range(len(records)))
    rng.shuffle(idx)
    chosen = idx[:limit]
    return [records[i] for i in chosen]


def infer_one(
    record: Dict[str, Any],
    cfg: Dict[str, Any],
    template_text: str,
    model_name: str,
) -> Tuple[Dict[str, Any], bool]:
    mode = cfg.get("llm", {}).get("mode", "openai_compatible")
    fallback_on_error = bool(cfg.get("runtime", {}).get("fallback_on_error", True))

    if mode == "heuristic":
        return heuristic_infer(record), False

    if mode != "openai_compatible":
        raise RuntimeError(f"Modo de benchmark no soportado: {mode}")

    cfg_for_model = copy.deepcopy(cfg)
    cfg_for_model.setdefault("llm", {}).setdefault("openai_compatible", {})["model"] = model_name

    try:
        inferred = openai_compatible_infer(record, cfg_for_model, template_text)
        return inferred, False
    except Exception as e:
        if not fallback_on_error:
            raise
        inferred = heuristic_infer(record)
        inferred["notes"] = (
            "Fallo openai_compatible en benchmark. Se uso fallback heuristico. "
            f"Error: {e}"
        )
        return inferred, True


def build_manual_review_rows(predictions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for p in predictions:
        rows.append(
            {
                "item_id": p.get("item_id", ""),
                "model": p.get("model", ""),
                "schema_name": p.get("schema_name", ""),
                "table_name": p.get("table_name", ""),
                "column_name": p.get("column_name", ""),
                "data_type": p.get("data_type", ""),
                "samples_preview": p.get("samples_preview", ""),
                "description": p.get("description", ""),
                "business_meaning": p.get("business_meaning", ""),
                "confidence": p.get("confidence", 0.0),
                "notes": p.get("notes", ""),
                "human_semantic_score_1_to_5": "",
                "human_description_score_1_to_5": "",
                "human_output_format_ok_0_or_1": "",
                "human_final_pass_0_or_1": "",
                "human_comments": "",
            }
        )
    return rows


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})


def run_benchmark(root: Path, cfg_path: Path) -> None:
    cfg = load_structured_file(cfg_path)

    samples_path = root / cfg["input"]["samples_path"]
    template_path = root / cfg["prompt"]["template_path"]
    output_dir = root / cfg["output"]["dir"]
    manual_review_csv = root / cfg["output"]["manual_review_csv"]
    summary_json = root / cfg["output"]["summary_json"]
    summary_csv = root / cfg["output"]["summary_csv"]

    all_records = read_jsonl(samples_path)
    records = select_records(all_records, cfg)
    template_text = template_path.read_text(encoding="utf-8")
    low_conf_threshold = float(cfg.get("evaluation", {}).get("low_confidence_threshold", 0.6))

    models = cfg.get("models", [])
    if not models:
        raise RuntimeError("No hay modelos definidos en config/benchmark.yaml")

    all_predictions: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []

    for model_idx, model_cfg in enumerate(models, start=1):
        model_name = model_cfg["name"]
        model_slug = slugify(model_name)
        model_predictions: List[Dict[str, Any]] = []

        fallback_count = 0
        for item_idx, record in enumerate(records, start=1):
            enriched = dict(record)
            enriched["database"] = cfg.get("context", {}).get("database", "")

            inferred, used_fallback = infer_one(
                enriched,
                cfg=cfg,
                template_text=template_text,
                model_name=model_name,
            )
            if used_fallback:
                fallback_count += 1

            pred = {
                "item_id": item_idx,
                "model": model_name,
                "schema_name": record.get("schema_name"),
                "table_name": record.get("table_name"),
                "column_name": record.get("column_name"),
                "data_type": record.get("data_type"),
                "samples_preview": json.dumps((record.get("samples", []) or [])[:5], ensure_ascii=False),
                "description": inferred.get("description", ""),
                "business_meaning": inferred.get("business_meaning", ""),
                "confidence": float(inferred.get("confidence", 0.0)),
                "notes": inferred.get("notes", ""),
            }
            model_predictions.append(pred)

        model_out = output_dir / f"predictions_{model_idx}_{model_slug}.jsonl"
        write_jsonl(model_out, model_predictions)

        confidences = [float(p.get("confidence", 0.0)) for p in model_predictions]
        low_conf_count = sum(1 for c in confidences if c < low_conf_threshold)
        avg_conf = (sum(confidences) / len(confidences)) if confidences else 0.0

        summary_rows.append(
            {
                "model": model_name,
                "records_evaluated": len(model_predictions),
                "avg_confidence": round(avg_conf, 4),
                "low_confidence_count": low_conf_count,
                "fallback_count": fallback_count,
                "output_path": str(model_out.relative_to(root)),
            }
        )
        all_predictions.extend(model_predictions)

    review_rows = build_manual_review_rows(all_predictions)
    write_csv(
        manual_review_csv,
        review_rows,
        fieldnames=list(review_rows[0].keys()) if review_rows else [
            "item_id",
            "model",
            "schema_name",
            "table_name",
            "column_name",
            "data_type",
            "samples_preview",
            "description",
            "business_meaning",
            "confidence",
            "notes",
            "human_semantic_score_1_to_5",
            "human_description_score_1_to_5",
            "human_output_format_ok_0_or_1",
            "human_final_pass_0_or_1",
            "human_comments",
        ],
    )

    write_csv(
        summary_csv,
        summary_rows,
        fieldnames=[
            "model",
            "records_evaluated",
            "avg_confidence",
            "low_confidence_count",
            "fallback_count",
            "output_path",
        ],
    )

    ensure_parent(summary_json)
    summary_payload = {
        "records_in_input": len(all_records),
        "records_evaluated": len(records),
        "models": summary_rows,
        "manual_review_csv": str(manual_review_csv.relative_to(root)),
        "summary_csv": str(summary_csv.relative_to(root)),
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    print(f"[benchmark] Resumen JSON: {summary_json}")
    print(f"[benchmark] Resumen CSV: {summary_csv}")
    print(f"[benchmark] Review CSV: {manual_review_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark de calidad para modelos LLM")
    parser.add_argument("--root", default=".", help="Ruta raiz del repo")
    parser.add_argument("--config", default="config/benchmark.yaml", help="Config de benchmark")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    cfg_path = (root / args.config).resolve()
    run_benchmark(root, cfg_path)


if __name__ == "__main__":
    main()
