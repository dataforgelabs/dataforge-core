
CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_distinct_many_join_query(in_source_id int, in_cte int)
    
 RETURNS text
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_sql text := '';
v_el meta.query_element;
v_group_by text;
v_agg_list text;

BEGIN

FOR v_el IN 
    SELECT * 
    FROM elements e
    WHERE e.cte = in_cte AND e.type = 'many-join'
    LOOP
        RAISE DEBUG 'Adding DIST/AGG queries for element %', to_json(v_el);
        PERFORM meta.u_assert( cardinality(v_el.many_join_list) >= 1, 'Many join list cardinality is 0 or null');
        
        v_sql := CASE WHEN v_sql = '' THEN '' ELSE v_sql || E',\n' END;
        -- DISTINCT query
        v_sql := v_sql || v_el.alias || '_DIST AS (SELECT DISTINCT ' || array_to_string(v_el.many_join_list,',') || 
        ' FROM ' || CASE WHEN in_cte = 0 THEN 'input' ELSE 'cte' || (in_cte - 1) END || '),
        ';
        
        -- Aggregate query
        -- list of GROUP BY attributes
        v_group_by := (SELECT string_agg('D.' || DL.att, ',') FROM unnest(v_el.many_join_list) DL(att) );
        PERFORM meta.u_assert( v_group_by IS NOT NULL,'Group by list is NULL');

        v_agg_list := (SELECT string_agg(e.expression || ' ' || e.alias, ',') 
        FROM elements e WHERE e.type = 'many-join attribute' AND e.parent_ids @> ARRAY[v_el.id] );
        PERFORM meta.u_assert( v_agg_list IS NOT NULL,'Aggregate list is NULL');
        
        v_sql := v_sql || v_el.alias || '_AGG AS (SELECT ' || v_group_by || ',' || v_agg_list
        || '
        FROM ' || v_el.alias || '_DIST D JOIN ' || CASE WHEN in_source_id = v_el.source_id AND in_cte > 0 THEN 'cte' || (in_cte - 1) -- self-join
        ELSE meta.u_get_hub_table_name(v_el.source_id) END || ' R ON '
        || v_el.expression || '
        GROUP BY ' || v_group_by || ')
        ';

END LOOP;

IF v_sql != '' AND in_cte > 0 THEN 
    v_sql := ',' || v_sql; 
END IF;

RETURN v_sql;

END;

$function$;