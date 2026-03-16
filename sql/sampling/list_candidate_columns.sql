SELECT
    c.table_schema AS schema_name,
    c.table_name,
  CASE
    WHEN t.table_type = 'BASE TABLE' THEN 'table'
    WHEN t.table_type = 'VIEW' THEN 'view'
    ELSE 'other'
  END AS relation_type,
    c.column_name,
    c.data_type,
    c.udt_name,
    c.ordinal_position
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema
 AND t.table_name = c.table_name
WHERE t.table_type IN ('BASE TABLE', 'VIEW')
  AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
  AND (
    c.data_type IN ('character varying', 'text', 'character')
    OR c.udt_name = 'citext'
  )
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
