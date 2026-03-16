#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
CONNECTIONS_FILE="config/connections.yaml"
PHASES_FILE="config/phases.yaml"
BENCHMARK_FILE="config/benchmark.yaml"
DEFAULT_MODEL="qwen2.5:14b"

load_env_if_exists() {
  if [[ -f ".env" ]]; then
    # shellcheck disable=SC1091
    source ".env"
  fi
}

check_python() {
  if [[ ! -x "${PYTHON_BIN}" ]]; then
    echo "[error] No existe ${PYTHON_BIN}."
    echo "[hint] Ejecuta: python3 -m venv .venv && source .venv/bin/activate && python -m pip install -r requirements.txt"
    return 1
  fi
}

run_phases_1_2() {
  load_env_if_exists
  check_python
  PYTHONPATH=src "${PYTHON_BIN}" run_pipeline.py --only 1,2 --connections "${CONNECTIONS_FILE}"
}

run_phases_3_4() {
  load_env_if_exists
  check_python
  PYTHONPATH=src "${PYTHON_BIN}" run_pipeline.py --only 3,4 --connections "${CONNECTIONS_FILE}"
}

run_full_1_4() {
  load_env_if_exists
  check_python
  PYTHONPATH=src "${PYTHON_BIN}" run_pipeline.py --root . --phases "${PHASES_FILE}" --connections "${CONNECTIONS_FILE}" --only 1,2,3,4
}

setup_llm() {
  load_env_if_exists
  local model="${LLM_MODEL:-$DEFAULT_MODEL}"
  echo "[info] Usando modelo: ${model}"
  ./scripts/llm/setup_ollama.sh "${model}"
}

check_llm() {
  load_env_if_exists
  ./scripts/llm/check_ollama_health.sh
}

run_benchmark() {
  load_env_if_exists
  check_python
  PYTHONPATH=src "${PYTHON_BIN}" run_benchmark.py --config "${BENCHMARK_FILE}"
}

show_outputs() {
  echo "\n[outputs]"
  ls -la output 2>/dev/null || echo "output/ aun no existe"
}

show_current_env() {
  load_env_if_exists
  echo "\n[env]"
  echo "PGHOST=${PGHOST:-<unset>}"
  echo "PGPORT=${PGPORT:-<unset>}"
  echo "PGDATABASE=${PGDATABASE:-<unset>}"
  echo "PGUSER=${PGUSER:-<unset>}"
  if [[ -n "${PGPASSWORD:-}" ]]; then
    echo "PGPASSWORD=<set>"
  else
    echo "PGPASSWORD=<unset>"
  fi
  echo "PGSSLMODE=${PGSSLMODE:-<unset>}"
  echo "LLM_ENDPOINT=${LLM_ENDPOINT:-<unset>}"
  if [[ -n "${LLM_API_KEY:-}" ]]; then
    echo "LLM_API_KEY=<set>"
  else
    echo "LLM_API_KEY=<unset>"
  fi
  echo "LLM_MODEL=${LLM_MODEL:-<unset>}"
}

menu() {
  while true; do
    echo "\n=== pg-autometadata menu ==="
    echo "1) Cargar y mostrar entorno (.env)"
    echo "2) Ejecutar fases 1+2 (discovery + sampling)"
    echo "3) Setup LLM local (Ollama pull)"
    echo "4) Health check LLM local"
    echo "5) Ejecutar fases 3+4 (inference + review)"
    echo "6) Ejecutar pipeline completo 1..4"
    echo "7) Ejecutar benchmark"
    echo "8) Ver carpeta output"
    echo "9) Salir"
    read -r -p "Selecciona opcion [1-9]: " opt

    case "${opt}" in
      1) show_current_env || echo "[error] Fallo opcion 1" ;;
      2) run_phases_1_2 || echo "[error] Fallo opcion 2" ;;
      3) setup_llm || echo "[error] Fallo opcion 3" ;;
      4) check_llm || echo "[error] Fallo opcion 4" ;;
      5) run_phases_3_4 || echo "[error] Fallo opcion 5" ;;
      6) run_full_1_4 || echo "[error] Fallo opcion 6" ;;
      7) run_benchmark || echo "[error] Fallo opcion 7" ;;
      8) show_outputs || echo "[error] Fallo opcion 8" ;;
      9) echo "Saliendo..."; break ;;
      *) echo "Opcion invalida" ;;
    esac
  done
}

menu
