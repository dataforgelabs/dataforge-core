CREATE OR REPLACE FUNCTION meta.u_sys_config(in_name text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$

BEGIN

RETURN CASE WHEN in_name = 'lakehouse-platform' THEN 'databricks' END;

END;
$BODY$;
