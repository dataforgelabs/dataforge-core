-- get datatype name from datatype_schema
CREATE OR REPLACE FUNCTION meta.u_get_typename_from_schema(in_schema jsonb)
    RETURNS text
    LANGUAGE 'plpgsql'
    
AS $BODY$

DECLARE

     v_type text;

BEGIN

v_type := CASE WHEN jsonb_typeof(in_schema) = 'string' THEN in_schema->>0
        WHEN jsonb_typeof(in_schema) = 'object' THEN meta.u_get_typename_from_schema(in_schema->'type') 
        END;


IF v_type IN ('integer', 'byte','short') THEN
    v_type := 'int';
ELSEIF v_type LIKE 'decimal%' THEN 
    v_type := 'decimal';
END IF;

RETURN v_type;

END;

$BODY$;