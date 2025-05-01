CREATE OR REPLACE FUNCTION meta.u_enr_query_add_sub_source_join(
    in_source_id int, -- this is final destination source we are building join chain to
    in_source_relation_ids int[],
    in_container_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql

AS $function$
DECLARE
    v_element_id int;
    v_parent_element_ids int[] := ARRAY[]::int[];
    v_parent_relation_ids int[];
    v_parameter meta.source_relation_parameter;
    v_parent_join_alias text;
    v_join_alias text;
    v_ret_element_id int;
    v_source_relation_id int := in_source_relation_ids[1];
    v_sr meta.source_relation;

BEGIN
    RAISE DEBUG 'Adding sub-source-join to source_id % for source_relation_ids % in source_container_id=%', in_source_id, in_source_relation_ids, in_container_source_id;

    PERFORM meta.u_assert( in_source_relation_ids IS NOT NULL AND cardinality(in_source_relation_ids) > 0, 'in_source_relation_ids is null or blank relation chain=' || in_source_relation_ids::text);

    -- chek if join already exists
    SELECT e.id INTO v_element_id
    FROM elements e
    WHERE e.type = 'sub-source-join' AND e.source_id = in_source_id AND e.relation_ids = in_source_relation_ids AND e.container_source_id = in_container_source_id;

    IF v_element_id IS NOT NULL THEN
        -- join already has been added - return element id
        RETURN v_element_id;
    END IF;

    PERFORM meta.u_assert( v_source_relation_id IS NOT NULL, 'source_relation_id is null in relation chain=' || in_source_relation_ids::text);

    -- Get relation details
    SELECT * INTO v_sr
    FROM meta.source_relation sr 
    WHERE sr.source_relation_id = v_source_relation_id;

    PERFORM meta.u_assert( v_sr.expression_parsed = 'implicit', 'expression_parsed is not implicit source_relation_id=' || v_source_relation_id);
    PERFORM meta.u_assert( v_sr.source_cardinality = '1', 'Join cardinality is not 1 for source_relation_id=' || v_source_relation_id || ' container_source_id=' || in_container_source_id);
    PERFORM meta.u_assert( v_sr.related_source_cardinality = 'M', 'Related join cardinality is not M for source_relation_id=' || v_source_relation_id || ' container_source_id=' || in_container_source_id);

    SELECT * INTO v_parameter
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id 
    AND p.source_relation_parameter_id = 1; -- only 1 system-created parameter should exist, pointing to sub-source enrichment

    PERFORM meta.u_assert( v_parameter.type IS NOT NULL, 'sub-source implicit relation parameter does not exist for source_relation_id=' || v_source_relation_id);
    -- v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);

    IF cardinality(in_source_relation_ids) = 1 THEN
        v_parent_join_alias := 'T' || v_parameter.source_id;
    ELSE 
        -- This is cascading join - need to add parent join to outer container_source_id
        v_parent_relation_ids :=  meta.u_remove_first_array_element(in_source_relation_ids);
        v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_source_id, v_parent_relation_ids, v_sr.source_id);
        v_parent_join_alias := 'J_' || meta.u_enr_query_relation_alias(v_parent_relation_ids);
    END IF;

    v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_source_relation_ids);

    -- Inserting the join record
    WITH cte AS (
    INSERT INTO elements ( type, source_id, expression, alias, attribute_id, parent_ids, relation_ids, container_source_id)
    VALUES ( 'sub-source-join', in_source_id, v_parent_join_alias || '.*' , v_join_alias, v_source_relation_id, v_parent_element_ids, in_source_relation_ids, in_container_source_id )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;