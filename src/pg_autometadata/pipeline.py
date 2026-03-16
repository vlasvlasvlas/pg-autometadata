import argparse
import csv
import json
import os
import re
from urllib import request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import psycopg
from psycopg import sql
import yaml


SUPPORTED_TEXT_TYPES = {
    "character varying",
    "text",
    "character",
    "citext",
}


@dataclass
class Phase:
    number: int
    value: str
    enabled: bool
    config: Path


def load_structured_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        if path.suffix.lower() == ".json":
            return json.load(f)
        return yaml.safe_load(f)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_conninfo(connections_cfg: Dict[str, Any], phase_cfg: Dict[str, Any]) -> str:
    conn_cfg = phase_cfg.get("connection", {})
    url_env = conn_cfg.get("url_env")
    if url_env:
        url = os.getenv(url_env)
        if not url:
            raise RuntimeError(f"Missing environment variable for connection URL: {url_env}")
        return url

    profile_name = conn_cfg.get("profile")
    if not profile_name:
        raise RuntimeError("connection.profile is required when connection.url_env is not set")

    profiles = connections_cfg.get("profiles", {})
    profile = profiles.get(profile_name)
    if not profile:
        raise RuntimeError(f"Connection profile not found: {profile_name}")

    host = profile.get("host", "localhost")
    port = profile.get("port", 5432)
    database = conn_cfg.get("database") or profile.get("database")
    user = profile.get("user")
    sslmode = profile.get("sslmode", "prefer")

    password_env = profile.get("password_env")
    password = os.getenv(password_env, "") if password_env else ""

    if not database:
        raise RuntimeError("Database name is required in profile or phase connection")
    if not user:
        raise RuntimeError("User is required in connection profile")

    return (
        f"host={host} port={port} dbname={database} user={user} "
        f"password={password} sslmode={sslmode}"
    )


def list_columns(conn: psycopg.Connection, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    query_path = Path(cfg["sql"]["list_columns_file"])
    query = query_path.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    records = [dict(zip(cols, r)) for r in rows]
    return apply_scope_filters(records, cfg)


def list_candidate_columns(conn: psycopg.Connection, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    query_path = Path(cfg["sql"]["list_candidates_file"])
    query = query_path.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    records = [dict(zip(cols, r)) for r in rows]
    return apply_scope_filters(records, cfg)


def apply_scope_filters(records: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    scope = cfg.get("scope", {})
    include_schemas = set(scope.get("include_schemas", []))
    exclude_schemas = set(scope.get("exclude_schemas", []))
    include_tables = set(scope.get("include_tables", []))
    exclude_tables = set(scope.get("exclude_tables", []))
    include_columns = set(scope.get("include_columns", []))
    exclude_columns = set(scope.get("exclude_columns", []))
    include_relations = set(scope.get("include_relations", []))

    type_filters = cfg.get("column_type_filters", {})
    include_types = set(type_filters.get("include_data_types", []))
    exclude_types = set(type_filters.get("exclude_data_types", []))

    out = []
    for r in records:
        schema_name = r.get("schema_name")
        table_name = r.get("table_name")
        column_name = r.get("column_name")
        data_type = r.get("data_type")
        udt_name = r.get("udt_name")
        relation_type = r.get("relation_type")

        if include_schemas and schema_name not in include_schemas:
            continue
        if schema_name in exclude_schemas:
            continue

        if include_tables and table_name not in include_tables:
            continue
        if table_name in exclude_tables:
            continue

        if include_columns and column_name not in include_columns:
            continue
        if column_name in exclude_columns:
            continue

        if include_relations and relation_type and relation_type not in include_relations:
            continue

        type_candidates = {str(data_type or "").lower(), str(udt_name or "").lower()}
        normalized_include_types = {str(x).lower() for x in include_types}
        normalized_exclude_types = {str(x).lower() for x in exclude_types}

        if normalized_include_types and type_candidates.isdisjoint(normalized_include_types):
            continue
        if type_candidates.intersection(normalized_exclude_types):
            continue

        out.append(r)

    return out


def write_inventory_csv(records: List[Dict[str, Any]], output_path: Path) -> None:
    ensure_parent(output_path)
    fields = [
        "schema_name",
        "table_name",
        "relation_type",
        "column_name",
        "data_type",
        "udt_name",
        "ordinal_position",
        "is_nullable",
        "column_default",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in records:
            writer.writerow({k: row.get(k) for k in fields})


def read_inventory_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def sample_column_values(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    column_name: str,
    sample_size: int,
    max_value_length: int,
    distinct_preferred: bool,
) -> List[str]:
    if distinct_preferred:
        query = sql.SQL(
            """
                        SELECT t.val
                        FROM (
                                SELECT DISTINCT LEFT(CAST({column} AS text), %s) AS val
                                FROM {schema}.{table}
                                WHERE {column} IS NOT NULL
                                    AND CAST({column} AS text) <> ''
                        ) AS t
                        ORDER BY random()
                        LIMIT %s
            """
        ).format(
            column=sql.Identifier(column_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        )
    else:
        query = sql.SQL(
            """
            SELECT LEFT(CAST({column} AS text), %s) AS val
            FROM {schema}.{table}
            WHERE {column} IS NOT NULL
              AND CAST({column} AS text) <> ''
            ORDER BY random()
            LIMIT %s
            """
        ).format(
            column=sql.Identifier(column_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        )

    with conn.cursor() as cur:
        cur.execute(query, (max_value_length, sample_size))
        rows = cur.fetchall()

    return [r[0] for r in rows if r and r[0] is not None]


def write_jsonl(path: Path, items: Iterable[Dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def heuristic_infer(record: Dict[str, Any]) -> Dict[str, Any]:
    column = record.get("column_name", "")
    samples = record.get("samples", []) or []

    joined = " ".join(samples[:10]).lower()
    column_lower = column.lower()

    if re.search(r"mail|email", column_lower) or re.search(r"@", joined):
        meaning = "Direccion de correo electronico"
        confidence = 0.9
    elif re.search(r"name|nombre", column_lower):
        meaning = "Nombre descriptivo"
        confidence = 0.8
    elif re.search(r"address|direccion", column_lower):
        meaning = "Direccion postal o ubicacion"
        confidence = 0.75
    elif re.search(r"phone|telefono|cel", column_lower):
        meaning = "Numero de contacto"
        confidence = 0.85
    elif re.search(r"status|estado", column_lower):
        meaning = "Estado o situacion de negocio"
        confidence = 0.7
    else:
        meaning = "Atributo textual de negocio"
        confidence = 0.55

    return {
        "description": f"Campo textual '{column}' en {record.get('schema_name')}.{record.get('table_name')}.",
        "business_meaning": meaning,
        "confidence": confidence,
        "notes": "Inferencia heuristica. Validar en revision humana si la confianza es baja.",
    }


def render_prompt(template: str, record: Dict[str, Any]) -> str:
    samples_json = json.dumps(record.get("samples", []), ensure_ascii=False)
    values = {
        "database": record.get("database", ""),
        "schema_name": record.get("schema_name", ""),
        "table_name": record.get("table_name", ""),
        "column_name": record.get("column_name", ""),
        "data_type": record.get("data_type", ""),
        "samples": samples_json,
    }
    return template.format(**values)


def extract_json_object(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json", "", 1).strip()

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("No valid JSON object found in model response")

    return json.loads(cleaned[start : end + 1])


def openai_compatible_infer(
    record: Dict[str, Any],
    cfg: Dict[str, Any],
    template_text: str,
) -> Dict[str, Any]:
    llm_cfg = cfg.get("llm", {}).get("openai_compatible", {})
    endpoint_env = llm_cfg.get("endpoint_env")
    api_key_env = llm_cfg.get("api_key_env")
    model = llm_cfg.get("model")
    temperature = float(llm_cfg.get("temperature", 0.1))
    max_tokens = int(llm_cfg.get("max_tokens", 400))

    if not endpoint_env:
        raise RuntimeError("Missing llm.openai_compatible.endpoint_env")
    if not api_key_env:
        raise RuntimeError("Missing llm.openai_compatible.api_key_env")
    if not model:
        raise RuntimeError("Missing llm.openai_compatible.model")

    endpoint = os.getenv(endpoint_env)
    api_key = os.getenv(api_key_env)
    if not endpoint:
        raise RuntimeError(f"Missing endpoint env var: {endpoint_env}")
    if not api_key:
        raise RuntimeError(f"Missing api key env var: {api_key_env}")

    prompt = render_prompt(template_text, record)
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": "You are a data dictionary assistant. Return strict JSON only."},
            {"role": "user", "content": prompt},
        ],
    }

    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")

    response_json = json.loads(body)
    content = (
        response_json.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    parsed = extract_json_object(content)

    return {
        "description": parsed.get("description", ""),
        "business_meaning": parsed.get("business_meaning", ""),
        "confidence": float(parsed.get("confidence", 0.0)),
        "notes": parsed.get("notes", ""),
    }


def run_discovery(connections_cfg: Dict[str, Any], cfg: Dict[str, Any], root: Path) -> None:
    conninfo = build_conninfo(connections_cfg, cfg)
    with psycopg.connect(conninfo) as conn:
        records = list_columns(conn, cfg)

    output_path = root / cfg["inventory"]["output_path"]
    write_inventory_csv(records, output_path)
    print(f"[1.discovery] Inventario generado: {output_path} ({len(records)} columnas)")


def run_sampling(connections_cfg: Dict[str, Any], cfg: Dict[str, Any], root: Path) -> None:
    conninfo = build_conninfo(connections_cfg, cfg)

    use_inventory = cfg.get("source", {}).get("use_inventory_file", True)
    if use_inventory:
        inventory_path = root / cfg["source"]["inventory_path"]
        candidates = read_inventory_csv(inventory_path)
    else:
        with psycopg.connect(conninfo) as conn:
            candidates = list_candidate_columns(conn, cfg)

    # Reusar filtros para evitar sorpresas si el inventario fue amplio.
    candidates = apply_scope_filters(candidates, cfg)

    sample_size = int(cfg["sampling"].get("sample_size", 50))
    max_value_length = int(cfg["sampling"].get("max_value_length", 200))
    distinct_preferred = bool(cfg["sampling"].get("distinct_preferred", True))
    random_seed = cfg["sampling"].get("random_seed")

    sampled: List[Dict[str, Any]] = []
    with psycopg.connect(conninfo) as conn:
        if random_seed is not None:
            seed = float(random_seed)
            if seed > 1 or seed < -1:
                seed = (abs(seed) % 10000) / 10000.0
            with conn.cursor() as cur:
                cur.execute("SELECT setseed(%s)", (seed,))

        for c in candidates:
            data_type = c.get("data_type", "")
            udt_name = c.get("udt_name", "")
            type_candidates = {str(data_type).lower(), str(udt_name).lower()}
            if type_candidates.isdisjoint(SUPPORTED_TEXT_TYPES):
                continue

            schema_name = c["schema_name"]
            table_name = c["table_name"]
            column_name = c["column_name"]
            values = sample_column_values(
                conn,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                sample_size=sample_size,
                max_value_length=max_value_length,
                distinct_preferred=distinct_preferred,
            )
            if not values:
                continue

            sampled.append(
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": data_type,
                    "udt_name": udt_name,
                    "samples": values,
                }
            )

    out_path = root / cfg["output"]["path"]
    write_jsonl(out_path, sampled)
    print(f"[2.sampling] Muestras generadas: {out_path} ({len(sampled)} columnas)")


def run_inference(cfg: Dict[str, Any], root: Path) -> None:
    in_path = root / cfg["input"]["samples_path"]
    out_path = root / cfg["output"]["path"]

    records = read_jsonl(in_path)
    mode = cfg.get("llm", {}).get("mode", "heuristic")
    template_path = root / cfg["prompt"]["template_path"]
    template_text = template_path.read_text(encoding="utf-8")

    results: List[Dict[str, Any]] = []
    for r in records:
        if mode == "heuristic":
            inf = heuristic_infer(r)
        elif mode == "openai_compatible":
            try:
                inf = openai_compatible_infer(r, cfg, template_text)
            except Exception as e:
                inf = heuristic_infer(r)
                inf["notes"] = (
                    "Fallo openai_compatible. Se uso fallback heuristico. "
                    f"Error: {e}"
                )
        else:
            inf = heuristic_infer(r)
            inf["notes"] = f"Modo no soportado: {mode}. Se uso fallback heuristico."

        merged = {
            "schema_name": r.get("schema_name"),
            "table_name": r.get("table_name"),
            "column_name": r.get("column_name"),
            "data_type": r.get("data_type"),
            "description": inf.get("description"),
            "business_meaning": inf.get("business_meaning"),
            "confidence": float(inf.get("confidence", 0.0)),
            "notes": inf.get("notes", ""),
        }
        results.append(merged)

    write_jsonl(out_path, results)
    print(f"[3.inference] Diccionario generado: {out_path} ({len(results)} atributos)")


def run_review(cfg: Dict[str, Any], root: Path) -> None:
    in_path = root / cfg["input"]["dictionary_path"]
    approved_path = root / cfg["output"]["approved_path"]
    needs_review_path = root / cfg["output"]["needs_review_path"]
    summary_path = root / cfg["output"]["summary_path"]

    threshold = float(cfg.get("review", {}).get("low_confidence_threshold", 0.6))
    records = read_jsonl(in_path)

    approved: List[Dict[str, Any]] = []
    needs_review: List[Dict[str, Any]] = []

    for r in records:
        conf = float(r.get("confidence", 0.0))
        if conf < threshold:
            needs_review.append(r)
        else:
            approved.append(r)

    write_jsonl(approved_path, approved)
    write_jsonl(needs_review_path, needs_review)

    ensure_parent(summary_path)
    summary = {
        "total": len(records),
        "approved": len(approved),
        "needs_review": len(needs_review),
        "low_confidence_threshold": threshold,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[4.review] Resumen generado: {summary_path}")


def parse_phases(phases_cfg: Dict[str, Any], root: Path, only: Optional[List[int]]) -> List[Phase]:
    out: List[Phase] = []
    for item in phases_cfg.get("phases", []):
        phase = Phase(
            number=int(item["number"]),
            value=str(item["value"]),
            enabled=bool(item.get("enabled", True)),
            config=root / item["config"],
        )
        if not phase.enabled:
            continue
        if only and phase.number not in only:
            continue
        out.append(phase)

    out.sort(key=lambda p: p.number)
    return out


def run_pipeline(root: Path, phases_file: Path, connections_file: Path, only: Optional[List[int]]) -> None:
    phases_cfg = load_structured_file(phases_file)
    connections_cfg = load_structured_file(connections_file)

    phases = parse_phases(phases_cfg, root, only)
    if not phases:
        raise RuntimeError("No hay fases habilitadas para ejecutar")

    for phase in phases:
        cfg = load_structured_file(phase.config)
        if phase.value == "discovery":
            run_discovery(connections_cfg, cfg, root)
        elif phase.value == "sampling":
            run_sampling(connections_cfg, cfg, root)
        elif phase.value == "inference":
            run_inference(cfg, root)
        elif phase.value == "review":
            run_review(cfg, root)
        elif phase.value == "benchmark":
            from pg_autometadata.benchmark import run_benchmark

            run_benchmark(root, phase.config)
        else:
            raise RuntimeError(f"Fase no soportada: {phase.number}.{phase.value}")


def main() -> None:
    parser = argparse.ArgumentParser(description="pg-autometadata pipeline runner")
    parser.add_argument(
        "--root",
        default=".",
        help="Ruta raiz del repo",
    )
    parser.add_argument(
        "--phases",
        default="config/phases.yaml",
        help="Archivo de fases (YAML o JSON)",
    )
    parser.add_argument(
        "--connections",
        default="config/connections.yaml",
        help="Archivo de conexiones (YAML o JSON)",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Lista de numeros de fase separados por coma. Ej: 1,2",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    phases_file = (root / args.phases).resolve()
    connections_file = (root / args.connections).resolve()

    only = None
    if args.only.strip():
        only = [int(x.strip()) for x in args.only.split(",") if x.strip()]

    run_pipeline(root, phases_file, connections_file, only)


if __name__ == "__main__":
    main()
