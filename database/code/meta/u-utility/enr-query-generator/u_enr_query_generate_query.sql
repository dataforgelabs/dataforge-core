CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_query(in_source_id int, in_mode text,
in_input_id int, in_enr_ids int[])
    
 RETURNS text
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_cte int;
v_sql text;
v_cte_max int;
v_many_join_query text;

BEGIN
PERFORM meta.u_assert( in_mode IN ('input', 'recalculation', 'reset'),'Unknown mode=' || in_mode);


IF in_mode = 'recalculation' AND in_enr_ids IS NULL THEN 
    -- This is recalculation phase triggered by new Input on [This] source: add window function enrichments with keep_current flag  
    SELECT array_agg(enrichment_id)
    INTO in_enr_ids
    FROM meta.enrichment e
    WHERE e.source_id = in_source_id AND e.active_flag 
    AND (e.window_function_flag AND e.keep_current_flag); 
    IF in_enr_ids IS NULL THEN
        RETURN NULL; -- nothing to do
    END IF;
END IF;

PERFORM meta.u_assert( CASE WHEN in_mode = 'input' THEN in_input_id IS NOT NULL ELSE true END,'input_id is NULL');

PERFORM meta.u_enr_query_generate_elements(in_source_id,in_mode, in_input_id, in_enr_ids);
PERFORM meta.u_assert( NOT EXISTS(SELECT 1 FROM elements WHERE cte IS NULL), 'Error calculating element CTE groups. Review output of meta.u_enr_query_generate_elements for details');
SELECT MAX(cte) INTO v_cte_max FROM elements;

v_sql := '/*Compiled on ' || now() || ' mode=' || in_mode || '*/
' || CASE WHEN v_cte_max > 0 OR EXISTS(SELECT 1 FROM elements e WHERE e.type = 'many-join attribute') THEN 'WITH ' ELSE '' END;

RAISE DEBUG '%', (SELECT json_agg(row_to_json(e)) FROM elements e);

FOR v_cte IN 0 .. v_cte_max LOOP
    v_many_join_query := meta.u_enr_query_generate_distinct_many_join_query(in_source_id, v_cte);
    RAISE DEBUG 'Many-join query %', v_many_join_query;
    v_sql := v_sql || CASE WHEN v_cte > 0 THEN E')\n' ELSE '' END || 
       v_many_join_query ||
       CASE WHEN v_cte > 0 AND v_cte < v_cte_max OR v_many_join_query != '' AND v_cte < v_cte_max THEN ',' ELSE '' END ||
       CASE WHEN v_cte < v_cte_max THEN ' cte' || v_cte || ' AS ( ' ELSE '' END || 'SELECT ';
    -- Add raw/system attributes 
    IF v_cte = 0 THEN
        v_sql := v_sql || COALESCE((SELECT string_agg( e.expression || 
        CASE WHEN e.expression = 'T.' || e.alias THEN '' ELSE ' ' || e.alias END,', ') 
        FROM elements e WHERE e.type IN ('raw','system') AND cte = 0), '');
        RAISE DEBUG 'Added raw attributes';
    ELSEIF v_cte = v_cte_max AND in_mode = 'input' THEN
        DELETE FROM elements WHERE type = 'system' AND alias = 's_input_id';
    END IF;

    -- Add attributes from prior CTEs
    v_sql := v_sql || COALESCE((SELECT string_agg('T.' || e.alias ,', ') 
    FROM elements e WHERE e.type IN ('raw','system','enrichment') AND e.cte < v_cte),'');
    -- Add current transits
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg(e.expression || ' ' || e.alias ,', ') 
    FROM elements e WHERE e.type like 'transit%' AND e.cte = v_cte
    ),'');
    -- Add prior transits required by downstream CTEs
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg('T.' || e.alias ,', ') 
    FROM elements e WHERE e.type like 'transit%' AND e.cte < v_cte
    AND EXISTS(SELECT 1 FROM elements fe WHERE fe.cte > v_cte AND fe.parent_ids @> ARRAY[e.id])),'');
   -- Add current CTE enrichments
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg( CASE WHEN e.data_type IS NOT NULL THEN 'CAST(' ELSE '' END ||
        e.expression || CASE WHEN e.data_type IS NOT NULL THEN ' AS ' || e.data_type || ')' ELSE '' END || ' ' || 
        CASE WHEN e.expression = 'T.' || e.alias THEN '' ELSE e.alias END ,', ') 
    FROM elements e WHERE e.type = 'enrichment' AND e.cte = v_cte),'');
    -- add FROM clause
    v_sql := v_sql || E'\nFROM ' || CASE WHEN v_cte = 0 THEN meta.u_get_source_table_name(in_source_id) ELSE 'cte' || (v_cte - 1) END
        || ' T';
   -- Add current CTE joins
    v_sql := v_sql || COALESCE((SELECT  string_agg( E'\nLEFT JOIN ' || CASE WHEN in_source_id = e.source_id AND v_cte > 0 THEN 'cte' || (v_cte - 1) -- self-join
        ELSE meta.u_get_hub_table_name(e.source_id) END || ' ' || e.alias 
                       || ' ON ' || e.expression,' ' ORDER BY e.alias) 
    FROM elements e WHERE e.type = 'join' AND e.cte = v_cte),'');
   -- Add current CTE many-joins
    v_sql := v_sql || COALESCE((SELECT  string_agg( E'\nLEFT JOIN ' || e.alias || '_AGG' 
                       || ' ON ' || 
                       -- Aggregate join list
                       (SELECT string_agg('T.' || DL.att || ' = ' || e.alias || '_AGG.' || DL.att, ' AND ') 
                        FROM unnest(e.many_join_list) DL(att) )
                       ,' ') 
    FROM elements e WHERE e.type = 'many-join' AND e.cte = v_cte),'');


END LOOP;


RETURN v_sql;
    

EXCEPTION WHEN OTHERS THEN
    RETURN 'QUERY GENERATION ERROR: ' || SQLERRM;

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_query(in_source_id int)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_hub_table_name text = meta.u_get_hub_table_name(in_source_id);
    v_sql text;
BEGIN
    v_sql := 'DROP TABLE IF EXISTS ' || v_hub_table_name || E';
    CREATE TABLE ' || v_hub_table_name || ' AS 
    ' || meta.u_enr_query_generate_query(in_source_id, 'input', 0, '{}'::int[]) ||  ';
    ';
    RETURN v_sql;
END;

$function$;