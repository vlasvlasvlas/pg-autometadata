-- Plantilla para muestreo por columna textual.
-- Reemplazar {{SCHEMA}}, {{TABLE}}, {{COLUMN}}, {{N}} desde tu runner.
SELECT
    t.val AS sample_value
FROM (
    SELECT
        LEFT(CAST({{COLUMN}} AS text), 200) AS val
    FROM {{SCHEMA}}.{{TABLE}}
    WHERE {{COLUMN}} IS NOT NULL
    ORDER BY random()
    LIMIT {{N}}
) AS t
WHERE t.val <> '';
