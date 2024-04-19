-- lookup complex attribute in source, then lookup key and return it's datatype or error
CREATE OR REPLACE FUNCTION meta.u_get_struct_key_datatype(in_datatype_schema jsonb, in_keys text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_key text;
    v_struct_keys_array text[];
    v_parameter parameter_map;
    v_schema jsonb := in_datatype_schema;
BEGIN

IF in_datatype_schema IS NULL THEN
    RETURN jsonb_build_object('error', 'Datatype schema is NULL');
END IF;

    IF COALESCE(in_keys,'') != '' THEN
    
        v_struct_keys_array := regexp_split_to_array(in_keys,'\.');

        FOREACH v_key IN ARRAY v_struct_keys_array LOOP
            RAISE DEBUG 'Key % schema %', v_key, v_schema;
            IF v_schema->>'type' IS DISTINCT FROM 'struct' THEN
                RETURN jsonb_build_object('error', format('Non-struct attribute cannot be accessed by .%s key',v_key));
            END IF;

            SELECT f->'type'
            INTO v_schema
            FROM jsonb_array_elements(v_schema->'fields') f
            WHERE f->>'name' = v_key;

            IF v_schema IS NULL THEN
                RETURN jsonb_build_object('error', format('Unable to lookup struct key `%s`. Check your expression',v_key));
            END IF;

        END LOOP;

    END IF;


    RETURN jsonb_build_object('datatype', COALESCE(v_schema->>'type',REPLACE(v_schema->>0,'integer','int')),
                              'datatype_schema',v_schema);

END;

$BODY$;
