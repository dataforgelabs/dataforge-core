-- print datatype schema in more readable format
CREATE OR REPLACE FUNCTION meta.u_print_datatype_schema(in_datatype_schema jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS
$BODY$


DECLARE
    v_fields_stripped json;
    v_datatype text;
BEGIN

v_datatype = meta.u_get_typename_from_schema(in_datatype_schema);

IF v_datatype = 'struct' THEN 

    SELECT jsonb_agg(jsonb_build_object('name', field->>'name', 'type',meta.u_print_datatype_schema(field->'type')))
    INTO v_fields_stripped
    FROM jsonb_array_elements(in_datatype_schema->'fields') field;

    RETURN in_datatype_schema || jsonb_build_object('fields', v_fields_stripped);

ELSEIF v_datatype = 'array' THEN 
    RETURN in_datatype_schema || jsonb_build_object('elementType', meta.u_print_datatype_schema(in_datatype_schema->'elementType'));

ELSE
   RETURN in_datatype_schema;
END IF;   


END;
$BODY$;

