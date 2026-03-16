#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if [[ -f "config/llm.local.env" ]]; then
  # shellcheck disable=SC1091
  source "config/llm.local.env"
elif [[ -f "config/llm.local.example.env" ]]; then
  echo "[run] No existe config/llm.local.env, usando defaults de example."
  # shellcheck disable=SC1091
  source "config/llm.local.example.env"
fi

if [[ -n "${LLM_MODEL:-}" ]]; then
  echo "[run] Ajustando modelo en config/inference.yaml -> ${LLM_MODEL}"
  sed -i.bak "s/^\([[:space:]]*model:[[:space:]]*\).*/\1${LLM_MODEL}/" config/inference.yaml
fi

PYTHONPATH=src .venv/bin/python run_pipeline.py --only 3,4 --connections config/connections.example.yaml

echo "[run] Listo. Revisar output/data_dictionary.jsonl y output/review_summary.json"
