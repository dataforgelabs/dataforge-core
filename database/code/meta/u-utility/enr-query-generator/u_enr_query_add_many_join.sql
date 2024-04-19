CREATE OR REPLACE FUNCTION meta.u_enr_query_add_many_join(in_many_join_source_id int, 
in_source_relation_ids int[])
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_parameter meta.source_relation_parameter;
v_cascading_source_id int;
v_join_alias text;
v_join_expression text;
v_ret_element_id int;
v_source_relation_id int := in_source_relation_ids[array_upper(in_source_relation_ids, 1)];
v_cascading_relation_ids int[] := in_source_relation_ids;
v_many_join_list text[] := '{}';
v_attribute_alias text;

BEGIN
RAISE DEBUG 'Adding many-join to source_id % for source_relation_ids %', in_many_join_source_id, in_source_relation_ids;
PERFORM meta.u_assert( v_source_relation_id IS NOT NULL, 'source_relation_id is null in relation chain=' || in_source_relation_ids::text);

-- chek if many-join already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'many-join' AND e.relation_ids = in_source_relation_ids;

IF v_element_id IS NOT NULL THEN
    -- sq already has been added - return element id
    RETURN v_element_id;
END IF;

-- Get relation expression
SELECT sr.expression_parsed
INTO v_join_expression
FROM meta.source_relation sr 
WHERE sr.source_relation_id = v_source_relation_id;

PERFORM meta.u_assert( v_join_expression IS NOT NULL, 'expression_parsed is NULL for source_relation_id=' || v_source_relation_id);

-- Process relation parameters: we only need to add parent links to [This] source parameters on the first Join in the chain
-- All cascading joins will not join to [This] and thus will have relation parameters ready
-- relation.expression_parsed := [This].P_<relation_arameter_id1> =  [Related].P_<relation_parameter_id2> + ... 
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id
    AND ( (p.source_id <> in_many_join_source_id AND p.self_relation_container IS NULL)
        OR (p.source_id = in_many_join_source_id AND p.self_relation_container IS NOT NULL)
    )
    -- exclude relation attributes of the many-join source as they are already available
    LOOP
        IF cardinality(in_source_relation_ids) = 1
            THEN
                    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);
        END IF;
        IF  v_parameter.source_id <> in_many_join_source_id OR v_parameter.self_relation_container = 'This' THEN
            v_attribute_alias := CASE WHEN cardinality(in_source_relation_ids) = 1 THEN  meta.u_enr_query_get_relation_parameter_name(v_parameter) 
            ELSE 'TR_' || v_parameter.source_relation_id || '_' || v_parameter.source_relation_parameter_id END;
            v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', 
            'D.' || v_attribute_alias);
            v_many_join_list := v_many_join_list || v_attribute_alias;
        END IF;
    END LOOP;

IF cardinality(in_source_relation_ids) > 1 THEN
     -- This is cascading many-join - need to add transits to parent join
    v_cascading_relation_ids[array_upper(v_cascading_relation_ids, 1)] := null;
    v_cascading_relation_ids = array_remove(v_cascading_relation_ids,null);
    SELECT CASE WHEN in_many_join_source_id = sr.source_id THEN sr.related_source_id ELSE sr.source_id END
    INTO v_cascading_source_id
    FROM meta.source_relation sr
    WHERE sr.source_relation_id = v_source_relation_id;

    FOR v_parameter IN 
        SELECT *
        FROM meta.source_relation_parameter p
        WHERE p.source_relation_id = v_source_relation_id
            AND p.source_id = v_cascading_source_id -- get all relation attributes of the cascading join source
        LOOP
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_parameter,v_cascading_relation_ids);
            v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', 
            'D.TR_' || v_parameter.source_relation_id || '_' || v_parameter.source_relation_parameter_id /* transit attribute alias*/);
        END LOOP;
END IF;

v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_source_relation_ids);

-- Find and replace expression parameters on the related side 
FOR v_parameter IN 
    SELECT *
    FROM meta.source_relation_parameter p
    WHERE p.source_relation_id = v_source_relation_id
    AND p.source_id = in_many_join_source_id AND (p.self_relation_container IS NULL OR 
        p.self_relation_container = 'Related')
    LOOP
        v_join_expression := replace(v_join_expression,'P<' || v_parameter.source_relation_parameter_id || '>', 
        'R.' || meta.u_enr_query_get_relation_parameter_name(v_parameter));
    END LOOP;

-- Inserting the many-join record
    WITH cte AS (
    INSERT INTO elements (type, source_id, expression, alias, attribute_id, parent_ids, relation_ids, many_join_list)
    VALUES ( 'many-join', in_many_join_source_id, v_join_expression, v_join_alias, null, v_parent_element_ids, in_source_relation_ids, v_many_join_list )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

RETURN v_ret_element_id;  
END;

$function$;