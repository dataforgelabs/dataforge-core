-- creates single column select expression for the output query
CREATE OR REPLACE  FUNCTION meta.u_add_backticks_output_column(in_output_column text)
    RETURNS TEXT
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_updated_name text;

BEGIN

    IF in_output_column !~ ('^[a-zA-Z_]+[a-zA-Z0-9_]*$') THEN
        RETURN '`' || in_output_column || '`';
    ELSE
        RETURN in_output_column;
    END IF;

END;
$function$;