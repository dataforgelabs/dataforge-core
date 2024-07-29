CREATE OR REPLACE FUNCTION meta.u_enr_query_add_join(
    in_source_id int, -- this is source we are building join to so we can use it's attribute
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
v_cascading_source_id int;
v_join_alias text;
v_parent_join_alias text;
v_join_expression text;
v_relation_source_id int;
v_attribute_name text;
v_attribute_alias text;
v_ret_element_id int;
v_source_relation_id int := in_source_relation_ids[array_upper(in_source_relation_ids, 1)];
v_cardinality text;
v_uv_enrichment_id int;
v_self_join_flag boolean;
v_join_container_source_id int := in_container_source_id;

BEGIN
RAISE DEBUG 'Adding join to source_id % for source_relation_ids % in source_container_id=%', in_source_id, in_source_relation_ids, in_container_source_id;
PERFORM meta.u_assert( in_source_relation_ids IS NOT NULL AND cardinality(in_source_relation_ids) > 0, 'in_source_relation_ids is null or blank relation chain=' || in_source_relation_ids::text);
PERFORM meta.u_assert( v_source_relation_id IS NOT NULL, 'source_relation_id is null in relation chain=' || in_source_relation_ids::text);

-- chek if join with same relation path already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type in ('join','sub-source-join') AND e.relation_ids = in_source_relation_ids AND e.container_source_id = in_container_source_id;

IF v_element_id IS NOT NULL THEN
    -- join already has been added - return element id
    RETURN v_element_id;
END IF;

-- check if first relation in the chain is implicit
SELECT sr.expression_parsed
INTO v_join_expression
FROM meta.source_relation sr 
WHERE sr.source_relation_id = in_source_relation_ids[1];

IF v_join_expression = 'implicit' THEN
    RETURN meta.u_enr_query_add_sub_source_join(in_source_id, in_source_relation_ids, in_container_source_id);
END IF;

-- Get relation expression
SELECT sr.expression_parsed, sr.source_id, CASE 
WHEN sr.source_id = sr.related_source_id THEN least(source_cardinality, related_source_cardinality)
WHEN sr.source_id = in_source_id THEN source_cardinality 
WHEN sr.related_source_id = in_source_id THEN related_source_cardinality END
INTO v_join_expression, v_relation_source_id, v_cardinality
FROM meta.source_relation sr 
WHERE sr.source_relation_id = v_source_relation_id ;

PERFORM meta.u_assert( v_join_expression IS NOT NULL, 'expression_parsed is NULL for source_relation_id=' || v_source_relation_id);
PERFORM meta.u_assert( v_cardinality IS NOT NULL, 'Relation source_relation_id=' || v_source_relation_id || ' is not attached to source_id=' || in_source_id);
PERFORM meta.u_assert( v_cardinality = '1', 'Join cardinality is not 1 for source_relation_id=' || v_source_relation_id || ' source_id=' || in_source_id);

-- Process relation parameters: we only need to add parent links to [This] source parameters on the first Join in the chain
-- All cascading joins will not join to [This] and thus will have relation parameters ready
-- relation.expression_parsed := [This].P_<relation_arameter_id1> =  [Related].P_<relation_parameter_id2> + ... 
IF cardinality(in_source_relation_ids) = 1 THEN
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = in_source_relation_ids[1]
        AND --p.source_id <> in_source_id -- exclude relation attributes of the target source as they are already available
        ( (p.source_id <> in_source_id AND p.self_relation_container IS NULL)
            OR (p.source_id = in_source_id AND p.self_relation_container IS NOT NULL)
        )
    LOOP
        PERFORM meta.u_assert( v_parameter.type IS NOT NULL, 'type is NULL for source_relation_parameter_id=' || v_parameter.source_relation_parameter_id);

        v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);
    END LOOP;
    v_parent_join_alias := 'T' || in_container_source_id;
ELSE 
    -- This is cascading join - need to add join parent
    SELECT CASE WHEN in_source_id = sr.source_id THEN sr.related_source_id ELSE sr.source_id END
    INTO v_cascading_source_id
    FROM meta.source_relation sr
    WHERE sr.source_relation_id =  v_source_relation_id;

    v_parent_relation_ids :=  meta.u_remove_last_array_element(in_source_relation_ids);
    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(v_cascading_source_id, v_parent_relation_ids,in_container_source_id);
    v_parent_join_alias := 'J_' || meta.u_enr_query_relation_alias(v_parent_relation_ids);
END IF;

v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_source_relation_ids);

-- Find and replace parameters
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id 
    LOOP
        v_attribute_name := meta.u_enr_query_get_relation_parameter_name(v_parameter);
        v_attribute_alias := CASE WHEN v_parameter.source_id <> in_source_id THEN v_parent_join_alias
            WHEN v_parameter.source_id = in_source_id AND (v_parameter.self_relation_container IS NULL OR v_parameter.self_relation_container = 'Related') THEN v_join_alias 
            ELSE v_parent_join_alias END;
        v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', v_attribute_alias || '.' || v_attribute_name);
    END LOOP;

-- Add Join unique filters
FOR v_attribute_name, v_uv_enrichment_id, v_self_join_flag IN 
    SELECT e.attribute_name, eu.enrichment_id, eu.source_id = in_source_id AND p.self_relation_container IS NOT NULL
    FROM meta.source_relation_parameter p JOIN meta.enrichment e ON p.enrichment_id = e.enrichment_id
    LEFT JOIN meta.enrichment eu ON eu.parent_enrichment_id = e.enrichment_id AND eu.attribute_name LIKE '%_uv_flag'
    WHERE p.source_relation_id = v_source_relation_id AND p.source_id = in_source_id
    AND p.type= 'enrichment' AND e.unique_flag
    LOOP
        PERFORM meta.u_assert( v_uv_enrichment_id IS NOT NULL, 'Uniqueness validation enrichment is missing or inactive for enrichment ' || v_attribute_name || ' referenced in for source_relation_id=' || v_source_relation_id);
        
        IF v_self_join_flag THEN -- add uniqueness enrichment for self-relation, forcing it to recalculate
            v_parent_element_ids := v_parent_element_ids || (SELECT meta.u_enr_query_add_enrichment(e) FROM meta.enrichment e WHERE e.enrichment_id = v_uv_enrichment_id);
        END IF;

        v_join_expression := v_join_expression || ' AND ' || v_join_alias || '.' 
         || v_attribute_name || '_uv_flag';
    END LOOP;    

-- Inserting the join record

    WITH cte AS (
    INSERT INTO elements ( type, source_id, expression, alias, attribute_id, parent_ids, relation_ids, container_source_id)
    VALUES ( 'join', in_source_id, v_join_expression, v_join_alias, v_source_relation_id, v_parent_element_ids, in_source_relation_ids, v_join_container_source_id )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;