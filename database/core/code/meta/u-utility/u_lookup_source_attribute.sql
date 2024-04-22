CREATE OR REPLACE FUNCTION meta.u_lookup_source_attribute(in_source_id int, in_attribute_name text, in_test_source_id int = null)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_ret parameter_map = ROW(null::text, null::int, null::int, null::int, in_source_id, null::text, null::text, null::jsonb)::parameter_map;
    v_attribute_name_substituted text;
    v_attribute_name_substituted_json json;
BEGIN

    v_attribute_name_substituted := in_attribute_name;
    
    SELECT r.raw_attribute_id, r.data_type, r.datatype_schema 
    INTO v_ret.raw_attribute_id, v_ret.datatype, v_ret.datatype_schema
    FROM meta.raw_attribute r 
    WHERE r.source_id = in_source_id AND r.column_alias = v_attribute_name_substituted;

    IF v_ret.raw_attribute_id IS NOT NULL THEN
        v_ret.type = 'raw';
        RETURN v_ret;
    END IF;

    SELECT enrichment_id, COALESCE(NULLIF(e.cast_datatype,''), e.datatype), datatype_schema 
    INTO v_ret.enrichment_id, v_ret.datatype, v_ret.datatype_schema
    FROM meta.enrichment e 
    WHERE e.source_id = in_source_id AND e.active_flag AND e.attribute_name = v_attribute_name_substituted;

    IF v_ret.enrichment_id IS NOT NULL THEN
        v_ret.type = 'enrichment';
        RETURN v_ret;
    END IF;

    SELECT s.system_attribute_id, s.data_type 
    INTO v_ret.system_attribute_id, v_ret.datatype
    FROM meta.system_attribute s
    JOIN meta.source src  ON src.source_id = in_source_id AND 
    s.refresh_type @> ARRAY[src.refresh_type] AND s.table_type @> ARRAY['hub']
    WHERE s.name = v_attribute_name_substituted;

    IF v_ret.system_attribute_id IS NOT NULL THEN
        v_ret.type = 'system';
        RETURN v_ret;
    END IF;

    v_ret.error := format('Attribute `%s` does not exist in source `%s`',v_attribute_name_substituted, meta.u_get_source_name(in_source_id)); 
    RETURN v_ret;

END;

$function$;


-- lookup complex attribute in source, then lookup key and return it's datatype or error
CREATE OR REPLACE FUNCTION meta.u_lookup_source_attribute(in_source_id int, in_attribute_name text, in_keys text)
    RETURNS parameter_map
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_key text;
    v_struct_keys_array text[];
    v_parameter parameter_map;
    v_schema jsonb;
BEGIN

    v_parameter := meta.u_lookup_source_attribute(in_source_id, in_attribute_name);

    IF v_parameter.error IS NOT NULL OR COALESCE(in_keys,'') = '' THEN
        RETURN v_parameter;
    END IF;
    
    v_schema := meta.u_get_struct_key_datatype(v_parameter.datatype_schema, in_keys);

    v_parameter.error = v_schema->>'error';
    v_parameter.datatype := v_schema->>'datatype';
    v_parameter.datatype_schema := v_schema->'datatype_schema';

    RETURN v_parameter;

END;

$BODY$;
