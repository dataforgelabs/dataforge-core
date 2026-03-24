CREATE OR REPLACE  FUNCTION meta.u_add_backticks(in_column text)
    RETURNS TEXT
    LANGUAGE sql
AS
$function$
SELECT CASE
    WHEN in_column !~ '^[a-zA-Z_]+[a-zA-Z0-9_]*$' THEN '`' || in_column || '`'
    ELSE in_column
END;
$function$;
