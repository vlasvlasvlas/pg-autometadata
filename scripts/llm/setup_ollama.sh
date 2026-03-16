#!/usr/bin/env bash
set -euo pipefail

MODEL="${1:-qwen2.5:14b}"

if ! command -v ollama >/dev/null 2>&1; then
  echo "[setup] No se encontro 'ollama'."
  echo "[setup] Instalar desde: https://ollama.com/download"
  exit 1
fi

echo "[setup] Asegurando que el servidor Ollama este activo..."
if ! curl -fsS "http://localhost:11434/api/tags" >/dev/null 2>&1; then
  echo "[setup] Servidor no responde. Intentando levantar 'ollama serve' en background..."
  nohup ollama serve >/tmp/ollama-serve.log 2>&1 &
  sleep 2
fi

echo "[setup] Descargando modelo ${MODEL} (puede tardar)..."
ollama pull "${MODEL}"

echo "[setup] Listo. Modelos locales:"
ollama list
