
CREATE OR REPLACE FUNCTION meta.u_get_schema_from_ddl_type(in_datatype text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    
AS $BODY$

DECLARE
    v_type text;

BEGIN

v_type := CASE WHEN in_datatype LIKE 'VARCHAR%' THEN 'VARCHAR'
    WHEN in_datatype LIKE 'TIMESTAMP%' THEN 'TIMESTAMP'
    WHEN in_datatype = 'NUMBER%' THEN  CASE WHEN in_datatype ~ ',0\)' THEN 'INTEGER' ELSE 'DECIMAL' END
    ELSE (SELECT hive_type FROM meta.attribute_type WHERE hive_ddl_type = in_datatype)
    END;

RETURN to_jsonb(v_type);


END;

$BODY$;