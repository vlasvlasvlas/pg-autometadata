#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${LLM_ENDPOINT:-http://localhost:11434/v1/chat/completions}"
BASE_URL="${ENDPOINT%/v1/chat/completions}"

echo "[health] Endpoint LLM: ${ENDPOINT}"

if curl -fsS "${BASE_URL}/api/tags" >/dev/null 2>&1; then
  echo "[health] OK: Ollama responde en ${BASE_URL}"
  echo "[health] Modelos disponibles:"
  curl -fsS "${BASE_URL}/api/tags" | sed 's/{/\n{/g' | head -n 20
else
  echo "[health] ERROR: Ollama no responde en ${BASE_URL}"
  echo "[health] Sugerencia: ejecutar scripts/llm/setup_ollama.sh qwen2.5:14b"
  exit 1
fi
