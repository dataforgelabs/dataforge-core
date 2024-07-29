
CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_elements(in_source_id int, in_mode text,
in_input_id int = null, in_enr_ids int[] = null)
 RETURNS SETOF meta.query_element
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_refresh_type text = 'full';
    v_starting_table_type text;
    v_cte int;
    v_enr_children int[];
    v_val_status_parents int[];
    v_raw_attribute_ids int[];
    v_input_id_rules_flag boolean;
    v_table_alias text = 'T' || in_source_id || '.';

BEGIN
    PERFORM meta.u_assert( in_mode IN ('input','recalculation','reset','sub-source'), 'Unknown mode=' || in_mode);
    PERFORM meta.u_assert( CASE WHEN in_mode = 'recalculation' THEN in_enr_ids IS NOT NULL 
    WHEN in_mode = 'input' THEN in_input_id IS NOT NULL ELSE true END, 'Required parameters not provided');

    IF in_mode != 'sub-source' THEN
        DROP TABLE IF EXISTS elements;
        CREATE TEMP TABLE elements (like meta.query_element INCLUDING ALL) ON COMMIT DROP;
    END IF;

    -- Add all row attributes 
    INSERT INTO elements ( type, expression, alias, attribute_id, cte, container_source_id)
    SELECT 'raw', v_table_alias || r.column_alias, r.column_alias, r.raw_attribute_id, 0, in_source_id 
    FROM meta.raw_attribute r 
    WHERE r.source_id = in_source_id;	

    -- Add all system attributes
    INSERT INTO elements ( type, expression, alias, attribute_id, cte, container_source_id)
    SELECT  'system', v_table_alias || sa.name, sa.name, system_attribute_id, 0, in_source_id
    FROM meta.system_attribute sa
    WHERE in_mode <> 'sub-source'; 

    -- add s_latest_flag for bulk reset CDC and input delete
    IF in_mode = 'reset' AND v_refresh_type = 'key' THEN
        INSERT INTO elements ( type, expression, alias, attribute_id, cte, container_source_id)
        VALUES  ('system', v_table_alias || 's_latest_flag', 's_latest_flag', 1000, 0, in_source_id );
    END IF;

    -- Add all enrichment attributes
    IF in_mode IN ('input','reset') THEN
        -- Get all keep_current window functions and their child enrichments
        SELECT COALESCE(array_agg(e.enrichment_id),'{}'::int[]) 
        INTO v_enr_children 
        FROM meta.enrichment e
        WHERE e.source_id = in_source_id AND e.active_flag 
        AND (e.window_function_flag AND e.keep_current_flag);
        IF cardinality(v_enr_children) > 0 THEN -- ADD children of window functions
            v_enr_children := v_enr_children || meta.u_enr_query_get_enrichment_children(in_source_id,v_enr_children);
        END IF;
        -- Add all enrichments that would not require recalculation
        PERFORM  meta.u_enr_query_add_enrichment(e)
        FROM meta.enrichment e
        WHERE e.source_id = in_source_id AND e.active_flag 
        AND e.enrichment_id <> ALL (v_enr_children);

    ELSEIF in_mode = 'recalculation' THEN
        v_enr_children := in_enr_ids || meta.u_enr_query_get_enrichment_children(in_source_id,in_enr_ids);

        -- add all pre-calculated enrichments, excluding those that need recalculation and their children
        INSERT INTO elements ( type, expression, alias, attribute_id, cte)
        SELECT 'enrichment', v_table_alias || e.attribute_name, e.attribute_name, e.enrichment_id, 0 
        FROM meta.enrichment e
        WHERE e.source_id = in_source_id AND e.active_flag
        AND e.enrichment_id <> ALL (v_enr_children);
        -- add enrichments requiring recalculation
        PERFORM  meta.u_enr_query_add_enrichment(e)
        FROM meta.enrichment e
        WHERE e.source_id = in_source_id AND e.active_flag
        AND e.enrichment_id = ANY (v_enr_children);

    ELSEIF in_mode = 'sub-source' THEN
        -- Get all enrichments
        PERFORM  meta.u_enr_query_add_enrichment(e)
        FROM meta.enrichment e
        WHERE e.source_id = in_source_id AND e.active_flag;

    END IF;

    -- assign cte to each element
    FOR v_cte IN 0 .. 1000 LOOP
        PERFORM meta.u_enr_query_update_cte(v_cte, in_mode, in_source_id);
        EXIT WHEN NOT EXISTS(SELECT 1 FROM elements e WHERE e.container_source_id = in_source_id AND e.cte IS NULL);
    END LOOP;

    RETURN QUERY SELECT * FROM elements;

END;

$function$;