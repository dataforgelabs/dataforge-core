CREATE OR REPLACE FUNCTION meta.u_get_output_parameter_name_id(in_parameter meta.output_source_column)
 RETURNS TABLE(attribute_name text, id int, source_id int) -- returns attribute name, id and source_id
 LANGUAGE plpgsql
 COST 10
AS $function$

BEGIN
    IF in_parameter.type = 'enrichment' THEN
        PERFORM meta.u_assert( in_parameter.enrichment_id IS NOT NULL, 'enrichment_id is NULL for output_source_column_id=' || in_parameter.output_source_column_id);
        RETURN QUERY
        SELECT e.attribute_name, e.enrichment_id, e.source_id 
        FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;
    ELSEIF in_parameter.type = 'raw' THEN
        PERFORM meta.u_assert( in_parameter.raw_attribute_id IS NOT NULL, 'raw_attribute_id is NULL for output_source_column_id=' || in_parameter.output_source_column_id);
        RETURN QUERY
        SELECT r.column_alias, r.raw_attribute_id, r.source_id
        FROM meta.raw_attribute r WHERE r.raw_attribute_id = in_parameter.raw_attribute_id;
    ELSEIF in_parameter.type = 'system' THEN
        PERFORM meta.u_assert( in_parameter.system_attribute_id IS NOT NULL, 'system_attribute_id is NULL for output_source_column_id=' || in_parameter.output_source_column_id);
        RETURN QUERY
        SELECT sy.name, sy.system_attribute_id, os.source_id 
        FROM meta.system_attribute sy JOIN meta.output_source os ON os.output_source_id = in_parameter.output_source_id
        WHERE sy.system_attribute_id = in_parameter.system_attribute_id;
    END IF;

RETURN;
END;

$function$;
