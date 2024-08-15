CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.enrichment_parameter, in_container_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_element_id int;
    v_parent_element_ids int[] := '{}';
    v_expression text;
    v_expression_alias text;
    v_transit_alias text;
    v_ret_element_id int;
    v_source_relation_ids int[]; 
    v_container_source_ids int[];
    v_parent_source_id int;

BEGIN
    v_transit_alias := 'TP_' || in_parameter.parent_enrichment_id || '_' || in_parameter.enrichment_parameter_id;
    SELECT e.id INTO v_element_id
    FROM elements e
    WHERE e.type = 'transit' AND e.attribute_id = in_parameter.enrichment_parameter_id AND e.alias = v_transit_alias
    AND in_parameter.source_relation_ids = e.relation_ids;

    IF v_element_id IS NOT NULL THEN
        -- enrichment already has been added - return element id
        RETURN v_element_id;
    END IF;
    RAISE DEBUG 'Adding transit for enrichment_parameter %', to_json(in_parameter);
    PERFORM meta.u_assert( cardinality(in_parameter.source_relation_ids) > 0, 'Relation chain cannot be blank for transit element. enrichment_parameter=' || to_json(in_parameter));

    -- remove leading sub-source joins
    SELECT source_relation_ids, container_source_ids, parent_source_id
    INTO v_source_relation_ids, v_container_source_ids, v_parent_source_id
    FROM meta.u_enr_query_add_transit_sub_source(in_parameter.source_relation_ids, in_container_source_id);

    IF v_source_relation_ids = '{}' THEN
        PERFORM meta.u_assert(v_parent_source_id IS NOT NULL, format('parent_source_id is NULL for parameter %s', to_json(in_parameter)));
        PERFORM meta.u_assert(cardinality(v_container_source_ids) > 0, format('container_source_ids is empty for parameter %s', to_json(in_parameter)));
        v_expression_alias := 'T' || v_parent_source_id;
    ELSE    
        -- Attribute from another source: add parent JOIN element
        v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_parameter.source_id, v_source_relation_ids, v_parent_source_id); 
        v_expression_alias := 'J_' || meta.u_enr_query_relation_alias(v_source_relation_ids);
    END IF;

    v_expression := v_expression_alias || '.' || meta.u_enr_query_get_enrichment_parameter_name(in_parameter);
    -- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids, source_id, container_source_id, container_source_ids)
    VALUES ( 'transit', v_expression, v_transit_alias, 
    in_parameter.enrichment_parameter_id, v_parent_element_ids, in_parameter.source_relation_ids, in_parameter.source_id, v_parent_source_id, v_container_source_ids)
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;  
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.source_relation_parameter,
in_relation_ids int[], in_container_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_element_id int;
    v_parent_element_ids int[] := '{}';
    v_expression text;
    v_expression_alias text;
    v_transit_alias text;
    v_ret_element_id int;
    v_source_relation_ids int[]; 
    v_container_source_ids int[];
    v_parent_source_id int;

BEGIN
    v_transit_alias := 'TR_' || in_parameter.source_relation_id || '_' || in_parameter.source_relation_parameter_id;

    SELECT e.id INTO v_element_id
    FROM elements e
    WHERE e.type = 'transit' AND e.alias = v_transit_alias
    AND in_relation_ids = e.relation_ids;

    IF v_element_id IS NOT NULL THEN
        -- enrichment already has been added - return element id
        RETURN v_element_id;
    END IF;

    RAISE DEBUG 'Adding transit for relation_parameter %', to_json(in_parameter);
    PERFORM meta.u_assert( cardinality(in_relation_ids) > 0, 'Relation chain cannot be blank for transit element. relation_parameter=' || to_json(in_parameter));

    -- remove leading sub-source joins
    SELECT source_relation_ids, container_source_ids, parent_source_id
    INTO v_source_relation_ids, v_container_source_ids, v_parent_source_id
    FROM meta.u_enr_query_add_transit_sub_source(in_relation_ids, in_container_source_id);

    IF v_source_relation_ids = '{}' THEN
        PERFORM meta.u_assert(v_parent_source_id IS NOT NULL, format('parent_source_id is NULL for parameter %s', to_json(in_parameter)));
        PERFORM meta.u_assert(cardinality(v_container_source_ids) > 0, format('container_source_ids is empty for parameter %s', to_json(in_parameter)));
        v_expression_alias := 'T' || v_parent_source_id;
    ELSE    
        -- Attribute from another source: add parent JOIN element
        v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_parameter.source_id, v_source_relation_ids, v_parent_source_id); 
        v_expression_alias := 'J_' || meta.u_enr_query_relation_alias(v_source_relation_ids);
    END IF;

    v_expression := v_expression_alias || '.' || meta.u_enr_query_get_enrichment_parameter_name(in_parameter);

    -- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids, source_id, container_source_id, container_source_ids)
    VALUES ( 'transit', v_expression, v_transit_alias, 
    in_parameter.source_relation_parameter_id, v_parent_element_ids, in_relation_ids, in_parameter.source_id, in_container_source_id , v_container_source_ids)
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.enrichment_aggregation, in_container_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_element_id int;
    v_parent_element_ids int[] := '{}';
    v_expression text;
    v_expression_alias text;
    v_transit_alias text;
    v_ret_element_id int;
    v_source_id int;
    v_source_relation_ids int[]; 
    v_container_source_ids int[];
    v_parent_source_id int;
BEGIN
    v_transit_alias := 'TA_' || in_parameter.enrichment_id || '_' || in_parameter.enrichment_aggregation_id;

    SELECT e.id INTO v_element_id
    FROM elements e
    WHERE e.type = 'transit-agg' AND e.attribute_id = in_parameter.enrichment_aggregation_id
    AND e.alias = v_transit_alias 
    AND in_parameter.relation_ids = e.relation_ids;

    IF v_element_id IS NOT NULL THEN
        -- enrichment already has been added - return element id
        RETURN v_element_id;
    END IF;

    RAISE DEBUG 'Adding transit for aggregation_parameter %', to_json(in_parameter);
    PERFORM meta.u_assert( cardinality(in_parameter.relation_ids) > 0, 'Relation chain cannot be blank for transit element. aggregation_parameter=' || to_json(in_parameter));

    -- remove leading sub-source joins
    SELECT source_relation_ids, container_source_ids, parent_source_id
    INTO v_source_relation_ids, v_container_source_ids, v_parent_source_id
    FROM meta.u_enr_query_add_transit_sub_source(in_parameter.relation_ids, in_container_source_id);

    PERFORM meta.u_assert( cardinality(v_source_relation_ids) > 0, 'Invalid relation chain for aggregation transit ' || to_json(in_parameter));


    -- Attribute from another source: add parent many-join attribute element
    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_many_join_attribute(in_parameter, v_source_relation_ids, v_parent_source_id);

    v_expression_alias := 'J_' || meta.u_enr_query_relation_alias(v_source_relation_ids);
    v_expression := v_expression_alias || '.A_' || in_parameter.enrichment_id || '_' || in_parameter.enrichment_aggregation_id;

    SELECT e.source_id INTO v_source_id
    FROM meta.enrichment e WHERE e.enrichment_id = in_parameter.enrichment_id;

    -- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids, source_id, container_source_id, container_source_ids)
    VALUES ( 'transit-agg', v_expression, v_transit_alias, 
    in_parameter.enrichment_aggregation_id, v_parent_element_ids, in_parameter.relation_ids,v_source_id, v_parent_source_id, v_container_source_ids)
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;