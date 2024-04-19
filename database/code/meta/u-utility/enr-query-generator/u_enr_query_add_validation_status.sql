 
CREATE OR REPLACE FUNCTION meta.u_enr_query_add_validation_status(
    in_source_id int)
 RETURNS int -- returns meta.query_element.id
 LANGUAGE plpgsql
 COST 100
AS $function$
DECLARE
    v_element_id int;
    v_val_status_parents int[];
    v_expression text;
    v_expression_fail text;
    v_expression_warn text;
BEGIN
RAISE DEBUG 'Adding s_validation_status_code to %', in_source_id;

-- chek if already exists
SELECT e.id INTO v_element_id
FROM elements e
WHERE e.type = 'enrichment' AND e.attribute_id = -1;

IF v_element_id IS NOT NULL THEN
    -- already has been added - return element id
    RETURN v_element_id;
END IF;

-- Get list of all validation rule elements
SELECT array_agg(el.id)
INTO v_val_status_parents
FROM meta.enrichment e LEFT JOIN elements el
ON e.enrichment_id = el.attribute_id AND el.type = 'enrichment'
WHERE e.source_id = in_source_id AND e.active_flag AND e.rule_type_code = 'V';

IF array_position(v_val_status_parents, NULL) IS NOT NULL THEN
    RAISE DEBUG 'Can''t add s_validation_status_code calculation, not all validation rules have been added yet.';
    RETURN null;
END IF;

-- All validations have been added - we can add status calculation
SELECT string_agg('T.' || el.alias, ' AND ') 
INTO v_expression_fail
FROM meta.enrichment e JOIN elements el
ON e.enrichment_id = el.attribute_id 
WHERE el.id = ANY(v_val_status_parents) AND e.validation_action_code = 'F';

SELECT string_agg('T.' || el.alias, ' AND ') 
INTO v_expression_warn
FROM meta.enrichment e JOIN elements el
ON e.enrichment_id = el.attribute_id 
WHERE el.id = ANY(v_val_status_parents) AND e.validation_action_code = 'W';

IF v_expression_fail IS NULL AND v_expression_warn IS NULL THEN 
    v_expression := 'CAST(''P'' as char(1))';
ELSE
    v_expression := 'CAST(CASE ' || COALESCE('WHEN NOT (' || v_expression_fail || ') THEN ''F'' ', '')
    || COALESCE('WHEN NOT (' || v_expression_warn || ') THEN ''W'' ','') || ' ELSE ''P'' END as char(1))';
END IF; 

WITH ct AS (
    INSERT INTO elements ( type, expression, alias, attribute_id, parent_ids, cte)
    VALUES ( 'enrichment', v_expression , 's_validation_status_code', -1, v_val_status_parents,
    CASE WHEN v_val_status_parents IS NULL THEN 0 END)
    RETURNING id )
    SELECT id INTO v_element_id FROM ct; 

RETURN v_element_id;
END;

$function$;