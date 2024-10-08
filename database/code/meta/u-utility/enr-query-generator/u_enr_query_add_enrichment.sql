CREATE OR REPLACE FUNCTION meta.u_enr_query_add_enrichment(in_enr meta.enrichment)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
v_element_id int;
v_parent_element_ids int[] := ARRAY[]::int[];
v_agg meta.enrichment_aggregation;
v_parameter meta.enrichment_parameter;
v_expression text;
v_ret_element_id int;

BEGIN
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'enrichment' AND e.attribute_id = in_enr.enrichment_id;

IF v_element_id IS NOT NULL THEN
    -- enrichment already has been added - return element id
    RETURN v_element_id;
END IF;

RAISE DEBUG 'Adding enrichment %', to_json(in_enr);
v_expression := in_enr.expression_parsed;
PERFORM meta.u_assert( v_expression IS NOT NULL, 'expression_parsed is NULL for enrichment=' || to_json(in_enr));
-- Process aggregate parameters
-- a.expression := AVG(P_<parameter_id1> + P_<parameter_id2> + ... 
-- + A_<aggregation_id1> + A_<aggregation_id2> ...
FOR v_agg IN 
    SELECT * 
    FROM meta.enrichment_aggregation a
    WHERE a.enrichment_id = in_enr.enrichment_id
    LOOP
        -- Attribute from another source: add parent TRANSIT element
        v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_agg); 
        -- update expression
        -- this assumes that transit element will be on earlier CTE level
        -- if enrichment and transit are going to be on the same level, replace T.TA_... expression with transit element expression
        v_expression := replace(v_expression,'A<' || v_agg.enrichment_aggregation_id || '>', 
        'T.TA_' || v_agg.enrichment_id || '_' || v_agg.enrichment_aggregation_id );

    END LOOP;
-- Process enrichment_parameters 
FOR v_parameter IN 
    SELECT *
    FROM meta.enrichment_parameter p
    WHERE p.parent_enrichment_id  = in_enr.enrichment_id AND p.aggregation_id IS NULL
    LOOP
        IF  v_parameter.source_id = in_enr.source_id AND v_parameter.self_relation_container IS NULL  THEN
                    v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_this_source_element(v_parameter);

            -- update expression
            v_expression := replace(v_expression,'P<' || v_parameter.enrichment_parameter_id || '>', 
            'T.' || meta.u_enr_query_get_enrichment_parameter_name(v_parameter) );
        ELSEIF (v_parameter.source_id <> in_enr.source_id OR v_parameter.self_relation_container IS NOT NULL) THEN
            -- Attribute from another source: add parent TRANSIT element
            v_parent_element_ids := v_parent_element_ids || meta.u_enr_query_add_transit(v_parameter); 
            -- update expression
            -- this assumes that transit element will be on earlier CTE level
            -- if enrichment and transit are going to be on the same level, replace T.TP_... expression with transit element expression
            v_expression := replace(v_expression,'P<' || v_parameter.enrichment_parameter_id || '>', 
            'T.TP_' || v_parameter.parent_enrichment_id || '_' || v_parameter.enrichment_parameter_id  );
        END IF;
    END LOOP;

-- We now have now added all parents of enrichment
-- Inserting the enrichment record

    WITH cte AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, data_type)
    VALUES ( 'enrichment', v_expression, in_enr.attribute_name, in_enr.enrichment_id, v_parent_element_ids,
            --Check for explicit casts or numeric/decimal types that need to be cast to 38,12
     CASE WHEN NULLIF(in_enr.cast_datatype,'') IS NOT NULL THEN 
        (SELECT at.hive_ddl_type FROM meta.attribute_type at WHERE at.hive_type = in_enr.cast_datatype)
     WHEN in_enr.datatype = 'decimal' THEN
        (SELECT at.hive_ddl_type FROM meta.attribute_type at WHERE at.hive_type = in_enr.datatype)
     END
     )
    RETURNING id )
    SELECT id INTO v_ret_element_id
    FROM cte;

RETURN v_ret_element_id;        
END;

$function$;