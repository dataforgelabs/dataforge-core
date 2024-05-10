-- creates single column select expression for the output query
CREATE OR REPLACE  FUNCTION meta.u_output_query_column_select(osc meta.output_source_column, in_hive_type text)
    RETURNS TEXT
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_attribute_name text;

BEGIN

    IF osc IS NULL THEN
        RETURN 
            CASE 
            WHEN in_hive_type = 'struct' THEN 'struct()'
            WHEN in_hive_type = 'array' THEN 'array()'
            WHEN in_hive_type IS NOT NULL THEN 'CAST(null as ' || in_hive_type || ')'
            ELSE 'null'
            END;
    END IF;

    SELECT attribute_name INTO v_attribute_name
    FROM meta.u_get_output_parameter_name_id(osc);

    PERFORM meta.u_assert(v_attribute_name IS NOT NULL,format('Unable to lookup attribute name for output mapping %s',osc));

    RETURN (  COALESCE(osc.aggregate || '(' || CASE WHEN osc.aggregate_distinct_flag THEN ' DISTINCT ' ELSE '' END, '') 
        ||  COALESCE('J_' || array_to_string(osc.source_relation_ids, '_'), 'T') -- alias
                || '.' 
                || v_attribute_name -- column name
                || CASE WHEN COALESCE(osc.keys,'') <> '' THEN '.' || osc.keys ELSE '' END -- struct key path
        || CASE WHEN osc.aggregate IS NOT NULL THEN ')' ELSE '' END 
    );

END;
$function$;
