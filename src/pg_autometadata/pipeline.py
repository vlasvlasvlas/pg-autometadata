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


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Soporte para valores entre comillas simples o dobles.
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]

        # Mantener prioridad de variables ya exportadas en la shell.
        os.environ.setdefault(key, value)


def load_local_env(root: Path) -> None:
    load_env_file(root / ".env")


def strip_sql_comments(query: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", "", query, flags=re.S)
    no_line = re.sub(r"--.*?$", "", no_block, flags=re.M)
    return no_line


def assert_select_only_query(query: str, source: str) -> None:
    cleaned = strip_sql_comments(query).strip()
    if not cleaned:
        raise RuntimeError(f"Empty SQL query is not allowed: {source}")

    normalized = cleaned.rstrip()
    if ";" in normalized[:-1]:
        raise RuntimeError(f"Only single-statement SELECT/WITH is allowed: {source}")

    head = normalized.lower().rstrip(";").lstrip()
    if not (head.startswith("select") or head.startswith("with")):
        raise RuntimeError(f"Only SELECT/WITH queries are allowed: {source}")

    forbidden = re.compile(
        r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|"
        r"call|execute|merge|vacuum|analyze|refresh|reindex|cluster|comment|"
        r"copy|do|set|reset)\b",
        flags=re.I,
    )
    if forbidden.search(head):
        raise RuntimeError(f"Detected non read-only SQL token in query: {source}")


def apply_read_only_guard(conn: psycopg.Connection, cfg: Dict[str, Any]) -> None:
    runtime_cfg = cfg.get("runtime", {})
    if not bool(runtime_cfg.get("force_read_only_connection", True)):
        return

    with conn.cursor() as cur:
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")


def get_profile_value(
    profile: Dict[str, Any],
    field: str,
    default: Optional[Any] = None,
) -> Any:
    env_field = f"{field}_env"
    env_name = profile.get(env_field)
    if env_name:
        env_value = os.getenv(str(env_name))
        if env_value is None or env_value == "":
            raise RuntimeError(f"Missing environment variable for profile field {field}: {env_name}")
        return env_value
    return profile.get(field, default)


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

    host = get_profile_value(profile, "host", "localhost")
    port = get_profile_value(profile, "port", 5432)
    database = conn_cfg.get("database") or get_profile_value(profile, "database")
    user = get_profile_value(profile, "user")
    sslmode = get_profile_value(profile, "sslmode", "prefer")

    password_env = profile.get("password_env")
    password = os.getenv(password_env, "") if password_env else ""

    if not database:
        raise RuntimeError("Database name is required in profile or phase connection")
    if not user:
        raise RuntimeError("User is required in connection profile")

    return (
        f"host={host} port={int(port)} dbname={database} user={user} "
        f"password={password} sslmode={sslmode}"
    )


def list_columns(conn: psycopg.Connection, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    query_path = Path(cfg["sql"]["list_columns_file"])
    query = query_path.read_text(encoding="utf-8")
    runtime_cfg = cfg.get("runtime", {})
    if bool(runtime_cfg.get("enforce_select_only", True)):
        assert_select_only_query(query, str(query_path))

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    records = [dict(zip(cols, r)) for r in rows]
    return apply_scope_filters(records, cfg)


def list_candidate_columns(conn: psycopg.Connection, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    query_path = Path(cfg["sql"]["list_candidates_file"])
    query = query_path.read_text(encoding="utf-8")
    runtime_cfg = cfg.get("runtime", {})
    if bool(runtime_cfg.get("enforce_select_only", True)):
        assert_select_only_query(query, str(query_path))

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
    max_len_literal = sql.Literal(int(max_value_length))
    sample_size_literal = sql.Literal(int(sample_size))

    if distinct_preferred:
        query = sql.SQL(
            """
                        SELECT t.val
                        FROM (
                SELECT DISTINCT LEFT(CAST({column} AS text), {max_len}) AS val
                                FROM {schema}.{table}
                                WHERE {column} IS NOT NULL
                                    AND CAST({column} AS text) <> ''
                        ) AS t
                        ORDER BY random()
            LIMIT {sample_size}
            """
        ).format(
            column=sql.Identifier(column_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            max_len=max_len_literal,
            sample_size=sample_size_literal,
        )
    else:
        query = sql.SQL(
            """
            SELECT LEFT(CAST({column} AS text), {max_len}) AS val
            FROM {schema}.{table}
            WHERE {column} IS NOT NULL
              AND CAST({column} AS text) <> ''
            ORDER BY random()
            LIMIT {sample_size}
            """
        ).format(
            column=sql.Identifier(column_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            max_len=max_len_literal,
            sample_size=sample_size_literal,
        )

    with conn.cursor() as cur:
        cur.execute(query)
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


def record_key(record: Dict[str, Any], fields: List[str]) -> str:
    return "||".join(str(record.get(f, "")) for f in fields)


def load_existing_jsonl_keys(path: Path, key_fields: List[str]) -> set[str]:
    if not path.exists():
        return set()
    return {record_key(item, key_fields) for item in read_jsonl(path)}


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
    # Reemplazo seguro de placeholders para no romperse con llaves JSON
    # presentes en el cuerpo del prompt (ejemplo: schema de salida).
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))
    return rendered


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
    timeout_seconds = int(llm_cfg.get("timeout_seconds", 60))

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
    with request.urlopen(req, timeout=timeout_seconds) as resp:
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
        apply_read_only_guard(conn, cfg)
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
    if not candidates:
        print(
            "[2.sampling] Sin candidatos despues de filtros. "
            "Revisar scope.include_schemas/include_tables/include_columns y type filters."
        )

    sample_size = int(cfg["sampling"]["sample_size"])
    max_value_length = int(cfg["sampling"]["max_value_length"])
    distinct_preferred = bool(cfg["sampling"].get("distinct_preferred", True))
    random_seed = cfg["sampling"].get("random_seed")

    out_path = root / cfg["output"]["path"]
    resume = bool(cfg.get("runtime", {}).get("resume", True))
    key_fields = ["schema_name", "table_name", "column_name"]

    existing_keys = load_existing_jsonl_keys(out_path, key_fields) if resume else set()
    file_mode = "a" if (resume and out_path.exists()) else "w"
    ensure_parent(out_path)

    written = 0
    skipped_existing = 0

    with psycopg.connect(conninfo) as conn:
        apply_read_only_guard(conn, cfg)
        if random_seed is not None:
            seed = float(random_seed)
            if seed > 1 or seed < -1:
                seed = (abs(seed) % 10000) / 10000.0
            with conn.cursor() as cur:
                cur.execute("SELECT setseed(%s)", (seed,))

        with out_path.open(file_mode, encoding="utf-8") as out_f:
            for c in candidates:
                data_type = c.get("data_type", "")
                udt_name = c.get("udt_name", "")
                type_candidates = {str(data_type).lower(), str(udt_name).lower()}
                if type_candidates.isdisjoint(SUPPORTED_TEXT_TYPES):
                    continue

                schema_name = c["schema_name"]
                table_name = c["table_name"]
                column_name = c["column_name"]

                key = record_key(
                    {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "column_name": column_name,
                    },
                    key_fields,
                )
                if key in existing_keys:
                    skipped_existing += 1
                    continue

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

                item = {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": data_type,
                    "udt_name": udt_name,
                    "samples": values,
                }
                out_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                out_f.flush()
                existing_keys.add(key)
                written += 1

    print(
        f"[2.sampling] Muestras en: {out_path} "
        f"(nuevas={written}, ya_existentes={skipped_existing})"
    )
    if written == 0 and skipped_existing == 0:
        print(
            "[2.sampling] No se generaron muestras. "
            "Puede no haber valores no nulos/no vacios o hay filtros muy restrictivos."
        )


def run_inference(cfg: Dict[str, Any], root: Path) -> None:
    in_path = root / cfg["input"]["samples_path"]
    out_path = root / cfg["output"]["path"]

    records = read_jsonl(in_path)
    if not records:
        print(
            f"[3.inference] Input vacio en {in_path}. "
            "Primero ejecutar 2.sampling o revisar filtros."
        )
        return
    mode = cfg.get("llm", {}).get("mode", "heuristic")
    template_path = root / cfg["prompt"]["template_path"]
    template_text = template_path.read_text(encoding="utf-8")

    resume = bool(cfg.get("runtime", {}).get("resume", True))
    show_progress = bool(cfg.get("runtime", {}).get("show_progress", True))
    progress_every = int(cfg.get("runtime", {}).get("progress_every", 1))
    progress_every = 1 if progress_every <= 0 else progress_every
    key_fields = ["schema_name", "table_name", "column_name"]
    existing_keys = load_existing_jsonl_keys(out_path, key_fields) if resume else set()
    file_mode = "a" if (resume and out_path.exists()) else "w"
    ensure_parent(out_path)

    written = 0
    skipped_existing = 0
    total = len(records)
    processed = 0

    with out_path.open(file_mode, encoding="utf-8") as out_f:
        for r in records:
            schema_name = str(r.get("schema_name", ""))
            table_name = str(r.get("table_name", ""))
            column_name = str(r.get("column_name", ""))
            location = f"{schema_name}.{table_name}.{column_name}"
            key = record_key(r, key_fields)
            if key in existing_keys:
                skipped_existing += 1
                processed += 1
                if show_progress and (processed % progress_every == 0 or processed == total):
                    pct = (processed / total) * 100 if total else 100.0
                    print(
                        f"[3.inference] {processed}/{total} ({pct:.2f}%) "
                        f"SKIP {location} | nuevos={written} ya_existentes={skipped_existing}",
                        flush=True,
                    )
                continue

            used_fallback = False
            status = "OK"
            if mode == "heuristic":
                inf = heuristic_infer(r)
                status = "HEURISTIC"
            elif mode == "openai_compatible":
                if show_progress:
                    pct = (processed / total) * 100 if total else 100.0
                    print(
                        f"[3.inference] {processed}/{total} ({pct:.2f}%) RUNNING {location}",
                        flush=True,
                    )
                try:
                    inf = openai_compatible_infer(r, cfg, template_text)
                except Exception as e:
                    inf = heuristic_infer(r)
                    used_fallback = True
                    inf["notes"] = (
                        "Fallo openai_compatible. Se uso fallback heuristico. "
                        f"Error: {e}"
                    )
                    status = "FALLBACK_HEURISTIC"
            else:
                inf = heuristic_infer(r)
                inf["notes"] = f"Modo no soportado: {mode}. Se uso fallback heuristico."
                status = "UNSUPPORTED_MODE_HEURISTIC"

            samples = r.get("samples", []) or []
            examples = [str(v) for v in samples if v is not None][:2]

            merged = {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
                "data_type": r.get("data_type"),
                "description": inf.get("description"),
                "business_meaning": inf.get("business_meaning"),
                "confidence": float(inf.get("confidence", 0.0)),
                "examples": examples,
                "notes": inf.get("notes", ""),
            }
            out_f.write(json.dumps(merged, ensure_ascii=False) + "\n")
            out_f.flush()
            existing_keys.add(key)
            written += 1
            processed += 1

            if show_progress and (processed % progress_every == 0 or processed == total):
                pct = (processed / total) * 100 if total else 100.0
                confidence = float(merged.get("confidence", 0.0))
                fallback_tag = " fallback=true" if used_fallback else ""
                print(
                    f"[3.inference] {processed}/{total} ({pct:.2f}%) "
                    f"{status} {location} conf={confidence:.2f}{fallback_tag} "
                    f"| nuevos={written} ya_existentes={skipped_existing}",
                    flush=True,
                )

    print(
        f"[3.inference] Diccionario en: {out_path} "
        f"(nuevos={written}, ya_existentes={skipped_existing})"
    )
    if written == 0 and skipped_existing == 0:
        print("[3.inference] No hubo filas para inferir.")


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
    load_local_env(root)

    phases_file = (root / args.phases).resolve()
    connections_file = (root / args.connections).resolve()

    only = None
    if args.only.strip():
        only = [int(x.strip()) for x in args.only.split(",") if x.strip()]

    run_pipeline(root, phases_file, connections_file, only)


if __name__ == "__main__":
    main()
