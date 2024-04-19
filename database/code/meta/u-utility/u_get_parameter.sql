CREATE OR REPLACE FUNCTION meta.u_get_parameter(in_p meta.enrichment_parameter)
 RETURNS parameter_map
 LANGUAGE plpgsql
AS $function$

DECLARE
       v_ret parameter_map = ROW(in_p.type, in_p.raw_attribute_id, in_p.enrichment_id, in_p.system_attribute_id, in_p.source_id, null::text, null::text, null::jsonb)::parameter_map;
BEGIN
    
    IF in_p.type = 'raw' THEN
            SELECT r.data_type, r.datatype_schema 
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = in_p.raw_attribute_id;
    ELSEIF in_p.type = 'enrichment' THEN
            SELECT e.datatype, e.datatype_schema  
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.enrichment e 
            WHERE e.enrichment_id = in_p.enrichment_id;
    ELSEIF in_p.type = 'system' THEN
            SELECT data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.datatype, v_ret.datatype_schema
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
       v_ret parameter_map = ROW(in_p.type, in_p.raw_attribute_id, in_p.enrichment_id, in_p.system_attribute_id, in_p.source_id, null::text, null::text, null::jsonb)::parameter_map;
BEGIN
    
    IF in_p.type = 'raw' THEN
            SELECT r.data_type, r.datatype_schema 
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.raw_attribute r 
            WHERE r.raw_attribute_id = in_p.raw_attribute_id;
    ELSEIF in_p.type = 'enrichment' THEN
            SELECT e.datatype, e.datatype_schema  
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.enrichment e 
            WHERE e.enrichment_id = in_p.enrichment_id;
    ELSEIF in_p.type = 'system' THEN
            SELECT data_type, meta.u_get_schema_from_type(null, data_type)
            INTO v_ret.datatype, v_ret.datatype_schema
            FROM meta.system_attribute s
            WHERE s.system_attribute_id = in_p.system_attribute_id;
    ELSE    
        v_ret.error := 'Invalid parameter ' || in_p::text;
    END IF;

    RETURN v_ret;

END;

$function$;