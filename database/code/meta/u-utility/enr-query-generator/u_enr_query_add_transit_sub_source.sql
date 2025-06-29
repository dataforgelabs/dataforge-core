CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit_sub_source(
    in_source_relation_ids int[], 
    in_container_source_id int)
 RETURNS TABLE (source_relation_ids int[], container_source_ids int[], parent_source_id int)
 -- Loop through in_source_relation_ids
 -- Remove implicit relations from the beginning of the chain
 -- build array of container source_ids corresponding to removed relations
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_source_relation_ids int[] = '{}';
    v_container_source_ids int[] = '{}';
    v_parent_source_id int := in_container_source_id; --parent source of sub-source
    v_source_relation_id int;
    v_sr meta.source_relation;
    v_next_source_id int = in_container_source_id;
    v_next_cardinality text;
    v_previous_source_id int;

BEGIN


FOREACH  v_source_relation_id IN ARRAY in_source_relation_ids LOOP
    SELECT * INTO v_sr 
    FROM meta.source_relation 
    WHERE source_relation_id = v_source_relation_id;

    IF v_sr.source_id = v_next_source_id THEN
        v_next_source_id = v_sr.related_source_id;
        v_previous_source_id = v_sr.source_id;
        v_next_cardinality := v_sr.related_source_cardinality;
    ELSE
        v_next_source_id = v_sr.source_id;
        v_next_cardinality := v_sr.source_cardinality;
        v_previous_source_id = v_sr.related_source_id;
    END IF;

    IF v_sr.expression_parsed = 'implicit' AND v_next_cardinality = '1' -- remove only implicit relations with M:1 cardinality
            AND v_source_relation_ids = '{}' -- only at the start of the chain
        THEN
        -- remove M:1 implicit relations at the start of the relation chain
        PERFORM meta.u_assert( v_sr.source_cardinality = '1', 'source_cardinality is not 1 for source_relation_id=' || v_source_relation_id || ' container_source_id=' || in_container_source_id);
        PERFORM meta.u_assert( v_sr.related_source_cardinality = 'M', 'related_source_cardinality is not M for source_relation_id=' || v_source_relation_id || ' container_source_id=' || in_container_source_id);
        v_container_source_ids := v_container_source_ids || v_sr.related_source_id;
        v_parent_source_id := v_sr.source_id;
    ELSE
        v_source_relation_ids := v_source_relation_ids || v_source_relation_id;
    END IF;

END LOOP;


RETURN QUERY SELECT v_source_relation_ids, v_container_source_ids, v_parent_source_id; 

END;

$function$;

