CREATE OR REPLACE FUNCTION meta.u_enr_query_get_relation_parameter_name(in_parameter meta.source_relation_parameter)
 RETURNS text -- returns attribute name
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_attribute_name text;
BEGIN
    IF in_parameter.type = 'enrichment' THEN
        PERFORM meta.u_assert( in_parameter.enrichment_id IS NOT NULL, 'enrichment_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT e.attribute_name INTO v_attribute_name
        FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'enrichment_id=' || in_parameter.enrichment_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'raw' THEN
        PERFORM meta.u_assert( in_parameter.raw_attribute_id IS NOT NULL, 'raw_attribute_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT r.column_alias INTO v_attribute_name
        FROM meta.raw_attribute r WHERE r.raw_attribute_id = in_parameter.raw_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'raw_attribute_id=' || in_parameter.raw_attribute_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    ELSEIF in_parameter.type = 'system' THEN
        PERFORM meta.u_assert( in_parameter.system_attribute_id IS NOT NULL, 'system_attribute_id is NULL for source_relation_parameter_id=' || in_parameter.source_relation_parameter_id);
        SELECT s.name INTO v_attribute_name
        FROM meta.system_attribute s WHERE s.system_attribute_id = in_parameter.system_attribute_id;
        PERFORM meta.u_assert( v_attribute_name IS NOT NULL, 'system_attribute_id=' || in_parameter.system_attribute_id || 'referenced by source_relation_parameter_id' || in_parameter.source_relation_parameter_id || ' does not exist');
    END IF;

RETURN v_attribute_name;
END;

$function$;