CREATE OR REPLACE FUNCTION meta.u_enr_query_add_many_join_attribute(in_agg meta.enrichment_aggregation, in_relation_ids int[], in_container_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := '{}';
v_attribute_name text;
v_attribute_alias text;
v_expression text := in_agg.expression;
v_parameter meta.enrichment_parameter;
v_ret_element_id int;
v_alias text := 'A_' || in_agg.enrichment_id || '_' || in_agg.enrichment_aggregation_id;

BEGIN
-- chek if attribute already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'many-join attribute' 
AND e.attribute_id = in_agg.enrichment_aggregation_id AND e.alias = v_alias AND e.container_source_id = in_container_source_id ;

IF v_element_id IS NOT NULL THEN
    -- attribute already has been added - return element id
    RETURN v_element_id;
END IF;

RAISE DEBUG 'Adding many-join attribute for enrichment_aggregation %', to_json(in_agg);
PERFORM meta.u_assert(in_agg.relation_ids IS NOT NULL, format('relation_ids is NULL for enrichment_aggregation %s container_source_id=%s',to_json(in_agg),in_container_source_id));
PERFORM meta.u_assert(in_relation_ids IS NOT NULL, format('in_relation_ids is NULL for enrichment_aggregation %s container_source_id=%s', to_json(in_agg), in_container_source_id));

v_parent_element_ids := v_parent_element_ids || 
    meta.u_enr_query_add_many_join(meta.u_enr_query_get_relation_chain_target_source_id(in_container_source_id,in_relation_ids),
    in_relation_ids, in_container_source_id);

-- process parameters of aggregation
FOR v_parameter IN 
    SELECT *
    FROM meta.enrichment_parameter p
    WHERE in_agg.enrichment_aggregation_id = p.aggregation_id AND p.parent_enrichment_id = in_agg.enrichment_id
    LOOP
    -- Get parameter name
        v_attribute_name := meta.u_enr_query_get_enrichment_parameter_name(v_parameter);
        -- attribute can come from :
        IF v_parameter.source_id = in_container_source_id AND v_parameter.self_relation_container IS NULL  THEN
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);


            v_attribute_alias := 'T';
        ELSEIF v_parameter.source_relation_ids = in_agg.relation_ids THEN
            -- attribute from many-join source: we already have many-join as a parent
            v_attribute_alias := 'R';
        ELSE
            -- This parameter is not from [This] or many-join source: add transit attribute
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_parameter, in_container_source_id);
            v_attribute_alias := 'T'; 
            v_attribute_name := 'TP_' || v_parameter.parent_enrichment_id || '_' || v_parameter.enrichment_parameter_id;
        END IF;
        
        v_expression := replace(v_expression,'P<' || v_parameter.enrichment_parameter_id || '>',
                v_attribute_alias || '.' || v_attribute_name);
    END LOOP;

-- Inserting the  record
    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, relation_ids, container_source_id)
    VALUES ('many-join attribute', v_expression, v_alias, 
            in_agg.enrichment_aggregation_id, v_parent_element_ids, in_agg.relation_ids, in_container_source_id)
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

    RETURN v_ret_element_id;
END;

$function$;