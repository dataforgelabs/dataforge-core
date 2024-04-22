CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.enrichment_parameter)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_expression text;
v_join_alias text;
v_transit_alias text;
v_ret_element_id int;

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
-- Attribute from another source: add parent JOIN element
v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_parameter.source_id, in_parameter.source_relation_ids); 
v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_parameter.source_relation_ids);

v_expression := v_join_alias || '.' || meta.u_enr_query_get_enrichment_parameter_name(in_parameter);
-- Inserting the transit record
WITH cte AS (
INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
VALUES ( 'transit', v_expression, v_transit_alias, 
in_parameter.enrichment_parameter_id, v_parent_element_ids, in_parameter.source_relation_ids )
RETURNING id )
SELECT id INTO v_ret_element_id
FROM cte;

RETURN v_ret_element_id;  
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.source_relation_parameter,
in_relation_ids int[])
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_expression text;
v_join_alias text;
v_transit_alias text;
v_ret_element_id int;
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

-- Attribute from another source: add parent JOIN element
v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_join(in_parameter.source_id, in_relation_ids); 
v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_relation_ids);

v_expression := v_join_alias || '.' || meta.u_enr_query_get_enrichment_parameter_name(in_parameter);

-- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
    VALUES ( 'transit', v_expression, v_transit_alias, 
    in_parameter.source_relation_parameter_id, v_parent_element_ids, in_relation_ids )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_transit(in_parameter meta.enrichment_aggregation)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_expression text;
v_join_alias text;
v_transit_alias text;
v_ret_element_id int;
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

-- Attribute from another source: add parent many-join attribute element

    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_many_join_attribute(in_parameter);
    
    v_join_alias := 'J_' || meta.u_enr_query_relation_alias(in_parameter.relation_ids) || '_AGG';
    v_expression := v_join_alias || '.A_' || in_parameter.enrichment_id || '_' || in_parameter.enrichment_aggregation_id;

-- Inserting the transit record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids)
    VALUES ( 'transit-agg', v_expression, v_transit_alias, 
    in_parameter.enrichment_aggregation_id, v_parent_element_ids, in_parameter.relation_ids )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;