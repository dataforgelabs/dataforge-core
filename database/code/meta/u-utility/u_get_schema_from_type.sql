-- build and return datatype_schema if it's null (legacy pre-8.0 attribute)
CREATE OR REPLACE FUNCTION meta.u_get_schema_from_type(in_schema jsonb, in_datatype text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    
AS $BODY$

DECLARE

     v_type text;

BEGIN

IF in_schema IS NOT NULL THEN
    IF jsonb_typeof(in_schema) = 'string' AND in_schema->>0 like 'decimal%' THEN
        RETURN to_jsonb('decimal(38,12)'::text);        
    ELSE
        RETURN in_schema;
    END IF;
END IF;


IF in_datatype = 'int' THEN
    RETURN to_jsonb('integer'::text);
ELSEIF in_datatype LIKE 'decimal%' THEN 
    RETURN to_jsonb('decimal(38,12)'::text);
END IF;

RETURN to_jsonb(in_datatype);

END;

$BODY$;