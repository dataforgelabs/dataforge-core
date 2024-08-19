CREATE OR REPLACE FUNCTION meta.u_get_parameter(in_p meta.enrichment_parameter)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
       v_ret parameter_map = ROW(null::text, in_p.type, in_p.raw_attribute_id, in_p.enrichment_id, in_p.system_attribute_id, in_p.source_id, null::text, null::text, null::jsonb)::parameter_map;
BEGIN
    
    IF in_p.type = 'raw' THEN
            SELECT r.column_alias, r.data_type, r.datatype_schema 
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = in_p.raw_attribute_id;
    ELSEIF in_p.type = 'enrichment' THEN
            SELECT e.attribute_name, e.datatype, meta.u_get_enrichment_datatype_schema(e)::jsonb datatype_schema
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.enrichment e 
            WHERE e.enrichment_id = in_p.enrichment_id;
    ELSEIF in_p.type = 'system' THEN
            SELECT s.name, data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.system_attribute s
            WHERE s.system_attribute_id = in_p.system_attribute_id;
    ELSE    
        v_ret.error := 'Invalid parameter ' || in_p::text;
    END IF;

    RETURN v_ret;

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_get_parameter(in_p meta.source_relation_parameter)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
       v_ret parameter_map = ROW(null::text, in_p.type, in_p.raw_attribute_id, in_p.enrichment_id, in_p.system_attribute_id, in_p.source_id, null::text, null::text, null::jsonb)::parameter_map;
BEGIN
    
    IF in_p.type = 'raw' THEN
            SELECT r.column_alias, r.data_type, r.datatype_schema 
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = in_p.raw_attribute_id;
    ELSEIF in_p.type = 'enrichment' THEN
            SELECT e.attribute_name, e.datatype, e.datatype_schema  
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.enrichment e 
            WHERE e.enrichment_id = in_p.enrichment_id;
    ELSEIF in_p.type = 'system' THEN
            SELECT s.name, data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.system_attribute s
            WHERE s.system_attribute_id = in_p.system_attribute_id;
    ELSE    
        v_ret.error := 'Invalid parameter ' || in_p::text;
    END IF;

    RETURN v_ret;

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_get_parameter(osc meta.output_source_column)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
       v_ret parameter_map = ROW(null::text, osc.type, osc.raw_attribute_id, osc.enrichment_id, osc.system_attribute_id, null, null::text, null::text, null::jsonb)::parameter_map;
       v_schema jsonb;
BEGIN
    

    IF osc.type = 'raw' THEN
            SELECT r.column_alias,r.data_type, r.datatype_schema, r.source_id 
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema, v_ret.source_id
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = osc.raw_attribute_id;
    ELSEIF osc.type = 'enrichment' THEN
            SELECT e.attribute_name, e.datatype,  meta.u_get_enrichment_datatype_schema(e)::jsonb datatype_schema, e.source_id  
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema, v_ret.source_id
            FROM meta.enrichment e 
            WHERE e.enrichment_id = osc.enrichment_id;
    ELSEIF osc.type = 'system' THEN
            SELECT s.name, data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.name, v_ret.datatype, v_ret.datatype_schema
            FROM meta.system_attribute s
            WHERE s.system_attribute_id = osc.system_attribute_id;

            SELECT os.source_id INTO v_ret.source_id 
            FROM meta.output_source os WHERE os.output_source_id = osc.output_source_id;
    ELSE    
        v_ret.error := 'Invalid parameter ' || osc::text;
    END IF;

    IF v_ret.error IS NOT NULL OR COALESCE(osc.keys,'') = '' THEN
        RETURN v_ret;
    END IF;
    
    -- update datatype and schema for struct key expression
    v_schema := meta.u_get_struct_key_datatype(v_ret.datatype_schema, osc.keys);

    v_ret.error = v_schema->>'error';
    v_ret.datatype := v_schema->>'datatype';
    v_ret.datatype_schema := v_schema->'datatype_schema';


    RETURN v_ret;

END;

$function$;