
CREATE OR REPLACE FUNCTION meta.u_output_generate_query(in_output_id INT)
    RETURNS TEXT
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_select_statement TEXT;
    v_from_statement   TEXT;
    v_where_clause     TEXT;
    v_origin_source_id INT;
    v_query            TEXT;
    v_queries           text[] = '{}';
    v_output_id        INT;
    v_output_parameters jsonb;
    v_include_pass_flag boolean;
    v_include_warn_flag boolean;
    v_include_fail_flag boolean;
    v_operation_type text;
    v_refresh_type text;
    v_input_filter text;
    v_output_source_id int;
    v_input_ids int[];
    v_system_fields text;
    v_source_id int;
    v_keep_current_flag boolean;
    v_aggregate_flag boolean;
    v_group_by text;
    v_output_type text;
    v_filter text;
    v_full_output_flag boolean;
    v_cte_structure text;
    v_output_subtype text;
    v_error text;
    v_output_source_ids int[];
    v_output_table text;
    v_output_schema text;
    v_output_full_table text;

BEGIN

SELECT o.output_type, o.output_id, o.output_sub_type, o.output_package_parameters->>'schema_name', output_package_parameters->>'table_name' 
INTO v_output_type, v_output_id, v_output_subtype, v_output_schema, v_output_table
FROM meta.output o 
WHERE o.output_id = in_output_id;

v_output_full_table := COALESCE(v_output_schema || '.', '') || v_output_table;

SELECT array_agg(os.output_source_id) INTO v_output_source_ids
FROM meta.output_source os WHERE os.output_id = in_output_id;

--Check for fields where the raw_attribute_id, enrichment_id or system_attribute_id has no corresponding record in the DB
with missing_fields AS (
    SELECT CASE WHEN osc.raw_attribute_id IS NOT NULL
                    THEN 'Raw Attribute'
                WHEN osc.enrichment_id IS NOT NULL
                    THEN 'Enrichment'
                WHEN osc.system_attribute_id IS NOT NULL
                    THEN 'System Attribute'
                ELSE 'No Attribute Mapped' END as field_type
           , COALESCE(osc.raw_attribute_id, COALESCE(osc.enrichment_id, osc.raw_attribute_id),0) as field_id
           , oc.name as column_name
           , osc.expression
    FROM meta.output_source_column osc
            JOIN meta.output_column oc ON osc.output_column_id = oc.output_column_id
             LEFT JOIN meta.raw_attribute ra ON osc.raw_attribute_id = ra.raw_attribute_id
             LEFT JOIN meta.enrichment e ON osc.enrichment_id = e.enrichment_id
             LEFT JOIN meta.system_attribute sa ON osc.system_attribute_id = sa.system_attribute_id
    WHERE osc.output_source_id = ANY(v_output_source_ids) AND ra.raw_attribute_id IS NULL AND e.enrichment_id IS NULL AND sa.system_attribute_id IS NULL
)

SELECT 'Columns mapped to missing fields detected: ' || array_agg(field_type || ' with ID ' || field_id || ' mapped into column ' || column_name || ' with expression ' || expression )::text || ' resave or redo these mappings to fix.'
INTO v_error
    FROM missing_fields;

IF v_error IS NOT NULL THEN
    RAISE EXCEPTION '%', v_error;
END IF;


IF v_output_type = 'virtual' THEN
    SELECT array_agg(os.output_source_id) INTO v_output_source_ids
    FROM meta.output_source os
    WHERE os.output_id = v_output_id;
END IF;

    FOREACH v_output_source_id IN ARRAY v_output_source_ids LOOP
        v_group_by := null;

    SELECT os.source_id, o.output_package_parameters || os.output_package_parameters, os.include_pass_flag, os.include_warn_flag, os.include_fail_flag, COALESCE(os.operation_type,'None'), s.refresh_type, s.source_id, os.filter, operation_type = 'Aggregate', COALESCE((o.output_package_parameters->>'full_output_flag')::boolean,false)
    INTO v_origin_source_id, v_output_parameters, v_include_pass_flag, v_include_warn_flag, v_include_fail_flag, v_operation_type, v_refresh_type, v_source_id, v_filter, v_aggregate_flag, v_full_output_flag
    FROM meta.output_source os
    JOIN meta.output o ON os.output_id = o.output_id
    JOIN meta.source s ON os.source_id = s.source_id
    WHERE output_source_id = v_output_source_id;

    v_keep_current_flag := EXISTS(SELECT 1 FROM meta.enrichment WHERE source_id = v_source_id AND keep_current_flag and active_flag);

    PERFORM meta.u_assert(v_include_pass_flag OR v_include_warn_flag OR v_include_fail_flag,'Must include at least one of Pass/Warn/Fail!');
    PERFORM meta.u_assert(v_operation_type<> 'Unpivot', 'Unpivot not supported yet!');


    v_system_fields :=  '';

    v_cte_structure :=  CASE WHEN v_aggregate_flag  AND v_output_type = 'table' THEN ' WITH agg_cte AS( ' ELSE '' END;
    SELECT string_agg(meta.u_output_query_column_select(osc, null) || ' as ' || meta.u_add_backticks(oc.name)
            , ', ' ORDER BY oc.position)
           || ' ' || CASE WHEN v_output_type <> 'table' THEN '' ELSE v_system_fields END
    INTO v_select_statement
    FROM meta.output_column oc
        JOIN meta.output o ON oc.output_id = o.output_id
             LEFT JOIN meta.output_source_column osc ON oc.output_column_id = osc.output_column_id AND osc.output_source_id = v_output_source_id
             LEFT JOIN meta.raw_attribute ra ON osc.raw_attribute_id = ra.raw_attribute_id
             LEFT JOIN meta.enrichment e ON osc.enrichment_id = e.enrichment_id
             LEFT JOIN meta.system_attribute sa ON osc.system_attribute_id = sa.system_attribute_id
    WHERE oc.output_id = v_output_id;


    v_from_statement := ' FROM ' || meta.u_get_hub_table_name(v_origin_source_id, (v_output_parameters->>'key_history')::boolean) || ' T';
    IF v_aggregate_flag THEN

            v_group_by  := ' GROUP BY ' || (SELECT string_agg(CASE WHEN osc.source_relation_ids IS NOT NULL THEN 'J_' || array_to_string(osc.source_relation_ids, '_') ELSE 'T' END || '.' || CASE osc.type WHEN 'raw' THEN ra.column_alias WHEN 'enrichment' THEN e.attribute_name WHEN 'system' THEN sa.name END,', ')
            FROM meta.output_source_column osc LEFT JOIN meta.raw_attribute ra ON osc.raw_attribute_id = ra.raw_attribute_id
             LEFT JOIN meta.enrichment e ON osc.enrichment_id = e.enrichment_id
             LEFT JOIN meta.system_attribute sa ON osc.system_attribute_id = sa.system_attribute_id
                WHERE osc.output_source_id = v_output_source_id AND osc.aggregate IS NULL)
                               --End the aggregate CTE
                               || CASE WHEN v_aggregate_flag  AND v_output_type = 'table' THEN ') SELECT * FROM agg_cte' ELSE '' END;

            v_input_filter := '';

    ELSEIF v_refresh_type = 'full' THEN
         v_input_filter := '';
    ELSE  v_input_filter := '';

     END IF;



    v_where_clause := ' WHERE true ' || COALESCE(' AND ' || REPLACE(NULLIF(v_filter,''),'[This]','T'),'');

    RAISE DEBUG 'SELECT: %', v_select_statement;
    RAISE DEBUG 'FROM: %', v_from_statement;
    RAISE DEBUG 'WHERE: %', v_where_clause;
    RAISE DEBUG 'INPUT: %', v_input_filter; 

    v_queries := v_queries || (v_cte_structure || 'SELECT ' || v_select_statement || v_from_statement || v_where_clause || CASE WHEN v_keep_current_flag OR v_output_type = 'virtual' OR v_full_output_flag THEN '' ELSE  v_input_filter END || COALESCE(v_group_by,''));

    END LOOP;

    v_query := 'DROP TABLE IF EXISTS ' || v_output_full_table || ';
    CREATE TABLE ' || v_output_full_table || ' AS 
    ' || array_to_string(v_queries, ' UNION ALL ');
    RETURN v_query;

EXCEPTION WHEN OTHERS THEN
    RETURN 'QUERY GENERATION ERROR: ' || SQLERRM;

END;


$function$;
