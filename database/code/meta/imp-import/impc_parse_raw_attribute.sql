CREATE OR REPLACE FUNCTION meta.impc_parse_raw_attribute(in_raw jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_parsed text[];
    v_attribute_name text;
    v_data_type text;
    v_datatype_schema jsonb;
BEGIN

    IF jsonb_typeof(in_raw) = 'string' THEN -- parse string
        v_parsed := regexp_split_to_array(in_raw->>0, '\s+(AS\s+|)','i');
        v_attribute_name := v_parsed[1];
        v_data_type := lower(v_parsed[2]);
        IF v_data_type IS NULL THEN
            RETURN jsonb_build_object('error', format('Unable to parse raw attribute data type from %s', in_raw));
        END IF;
        v_datatype_schema := meta.u_get_schema_from_type(null, v_data_type);

    ELSEIF jsonb_typeof(in_raw) = 'object' THEN -- parse object
        v_attribute_name := in_raw->>'name';
        v_datatype_schema := in_raw->'schema';
        v_data_type := meta.u_get_typename_from_schema(v_datatype_schema);
    ELSE
        RETURN jsonb_build_object('error', format('Invalid raw attribute spec %s', in_raw));
    END IF;

-- validate name and type

    IF v_attribute_name IS NULL THEN
        RETURN jsonb_build_object('error', format('Unable to parse raw attribute name from %s', in_raw));
    END IF;

    IF v_data_type NOT IN (SELECT hive_type from meta.attribute_type) THEN
        RETURN jsonb_build_object('error', format('Invalid raw attribute datatype `%s`', v_data_type));
    END IF;

    RETURN jsonb_build_object('name', lower(v_attribute_name), 'data_type', v_data_type, 'datatype_schema', v_datatype_schema);

END;

$BODY$;

