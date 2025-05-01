CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_query(in_source_id int, in_mode text, in_input_id int = null, in_enr_ids int[] = null)
 RETURNS text
 LANGUAGE plpgsql
SECURITY DEFINER
AS $function$

DECLARE
    v_cte int;
    v_sql text;
    v_cte_max int;
    v_many_join_query text;
    v_err_text text;
    v_err_detail text;
    v_err_context text;
    v_table_alias text = 'T' || in_source_id;

BEGIN
PERFORM meta.u_assert( in_mode IN ('input', 'recalculation', 'reset','sub-source'),'Unknown mode=' || in_mode);

IF in_mode = 'sub-source' AND NOT EXISTS(SELECT 1 FROM meta.enrichment e WHERE e.active_flag AND e.source_id = in_source_id) THEN 
    -- don't generate query when no rule exist in sub-source
    RETURN '';
END IF;

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

PERFORM meta.u_enr_query_generate_elements(in_source_id, in_mode, in_input_id, in_enr_ids);
PERFORM meta.u_assert( NOT EXISTS(SELECT 1 FROM elements e WHERE e.container_source_id = in_source_id AND cte IS NULL), 
'Error calculating element CTE groups. Review output of meta.u_enr_query_generate_elements for details');
SELECT MAX(cte) INTO v_cte_max FROM elements e WHERE e.container_source_id = in_source_id;


v_sql := CASE WHEN in_mode != 'sub-source' THEN '/*Compiled on ' || now() || ' mode=' || in_mode  || '*/
' ELSE '' END  || CASE WHEN v_cte_max > 0 THEN 'WITH ' ELSE '' END;

FOR v_cte IN 0 .. v_cte_max LOOP
    v_sql := v_sql || CASE WHEN v_cte > 0 THEN E')\n' ELSE '' END || 
       CASE WHEN v_cte > 0 AND v_cte < v_cte_max AND v_cte < v_cte_max THEN ',' ELSE '' END ||
       CASE WHEN v_cte < v_cte_max THEN ' cte' || v_cte || ' AS ( ' ELSE '' END || 'SELECT ' || 
       CASE WHEN in_mode = 'sub-source' AND v_cte = v_cte_max THEN E'ARRAY_AGG(STRUCT(\n' ELSE '' END;
    -- Add raw/system attributes 
    IF v_cte = 0 THEN
        v_sql := v_sql || COALESCE((SELECT string_agg( (e.expression || 
        CASE WHEN e.expression = v_table_alias  || '.' || e.alias THEN '' ELSE ' ' || e.alias END),', '  ORDER BY e.alias) 
        FROM elements e WHERE e.container_source_id = in_source_id AND e.type IN ('raw','system') AND cte = 0), '');
        RAISE DEBUG 'Added raw attributes';
    ELSEIF v_cte = v_cte_max AND in_mode = 'input' THEN
        DELETE FROM elements e WHERE e.container_source_id = in_source_id AND e.type = 'system' AND e.alias = 's_input_id';
    END IF;

    -- Add attributes from prior CTEs
    v_sql := v_sql || COALESCE((SELECT string_agg(v_table_alias || '.' || e.alias ,', '  ORDER BY e.alias) 
    FROM elements e WHERE e.container_source_id = in_source_id AND e.type IN ('raw','system') AND e.cte < v_cte),'');
    -- Add current transits
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg(e.expression || ' ' || e.alias ,', ') 
    FROM elements e WHERE e.container_source_id = in_source_id AND e.type like 'transit%' AND e.cte = v_cte AND NULLIF(e.container_source_ids,'{}') IS NULL
    ),'');
    
    -- Add prior transits required by downstream CTEs
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg(v_table_alias || '.' || e.alias ,', ') 
    FROM elements e WHERE e.container_source_id = in_source_id AND e.type like 'transit%' AND e.cte < v_cte AND NULLIF(e.container_source_ids,'{}') IS NULL
    AND EXISTS(SELECT 1 FROM elements fe WHERE fe.container_source_id = in_source_id AND fe.cte > v_cte AND fe.parent_ids @> ARRAY[e.id])),'');
  
   -- Add current and prior CTE enrichments
    v_sql := v_sql || COALESCE(', ' || (SELECT string_agg( 
            CASE WHEN e.cte < v_cte THEN v_table_alias || '.' || e.alias ELSE
                CASE WHEN e.data_type IS NOT NULL THEN 'CAST(' ELSE '' END ||
                e.expression || CASE WHEN e.data_type IS NOT NULL THEN ' AS ' || e.data_type || ')' ELSE '' END || ' ' || 
                CASE WHEN e.expression = v_table_alias || '.' || e.alias THEN '' ELSE e.alias END 
            END,
        ', '  ORDER BY e.alias) 
    FROM elements e WHERE e.container_source_id = in_source_id AND e.type = 'enrichment' AND e.cte <= v_cte),'');
    -- add FROM clause
    v_sql := v_sql || CASE WHEN in_mode = 'sub-source' AND v_cte = v_cte_max THEN E'))\n' ELSE '' END || 
            E'\nFROM ' || CASE WHEN v_cte = 0 THEN meta.u_get_source_table_name(in_source_id) ELSE 'cte' || (v_cte - 1) END
        || ' ' || v_table_alias;
   -- Add current CTE joins
    v_sql := v_sql || COALESCE((SELECT  string_agg( E'\nLEFT JOIN ' || CASE WHEN in_source_id = e.source_id AND v_cte > 0 AND cardinality(e.relation_ids) = 1 THEN 'cte' || (v_cte - 1) -- self-join
        ELSE meta.u_get_hub_table_name(e.source_id) END || ' ' || e.alias 
                       || ' ON ' || e.expression,' ' ORDER BY e.alias) 
    FROM elements e WHERE e.container_source_id = in_source_id AND e.type = 'join' AND e.cte = v_cte),'');

   -- Add current CTE many-joins
    v_sql := v_sql || meta.u_enr_query_generate_many_joins(in_source_id, v_cte);


END LOOP;

IF EXISTS (SELECT 1 FROM meta.source where source_id = in_source_id AND processing_type = 'stream') THEN
    UPDATE meta.process SET parameters = parameters || jsonb_build_object('query', v_sql) WHERE input_id = in_input_id 
    AND operation_type = 'stream';
END IF;

RETURN v_sql;
    

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_text = MESSAGE_TEXT,
                          v_err_detail = PG_EXCEPTION_DETAIL,
                          v_err_context = PG_EXCEPTION_CONTEXT;
    RETURN format('QUERY GENERATION ERROR: meta.u_enr_query_generate_query(%s,%L,%L ,%L)  %s DETAILS: %s CONTEXT: %s', 
     in_source_id, in_mode, in_input_id, in_enr_ids,
     v_err_text, v_err_detail,v_err_context);

END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_query(in_source_id int)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_hub_table_name text = meta.u_get_hub_table_name(in_source_id);
    v_query text;
BEGIN

    v_query := meta.u_enr_query_generate_query(in_source_id, 'input', 0, '{}'::int[]);
    IF v_query LIKE 'QUERY GENERATION ERROR:%' THEN
        RETURN v_query;
    ELSE
        RETURN ( 'DROP TABLE IF EXISTS ' || v_hub_table_name || E';
        CREATE TABLE ' || v_hub_table_name || ' AS 
        ' || v_query ||  ';
        ');
    END IF;

END;

$function$;