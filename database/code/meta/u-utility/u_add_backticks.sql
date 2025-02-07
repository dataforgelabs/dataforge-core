CREATE OR REPLACE  FUNCTION meta.u_add_backticks(in_column text)
    RETURNS TEXT
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_updated_name text;

BEGIN

    IF in_column !~ ('^[a-zA-Z_]+[a-zA-Z0-9_]*$') THEN
        RETURN '`' || in_column || '`';
    ELSE
        RETURN in_column;
    END IF;

END;
$function$;