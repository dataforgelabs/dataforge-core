CREATE OR REPLACE FUNCTION meta.impc_update_test_results(in_import_id int, in_res json)
    RETURNS json
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_err jsonb;
    v_imp meta.import;
    v_next json;

BEGIN

SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

CREATE TEMP TABLE _test_result ON COMMIT DROP AS
SELECT (e->>'source_relation_id')::int source_relation_id, (e->>'enrichment_id')::int enrichment_id, (e->>'output_source_id')::int output_source_id,  e->'result' result
FROM json_array_elements(COALESCE(in_res,'[]'::json)) e;

           
UPDATE meta.relation_test t
SET result = r.result
FROM _test_result r 
WHERE t.source_relation_id = r.source_relation_id AND t.project_id = v_imp.project_id;

UPDATE meta.enrichment_test t
SET result = r.result
FROM _test_result r 
WHERE t.enrichment_id = r.enrichment_id AND t.project_id = v_imp.project_id;

UPDATE meta.output_filter_test t
SET result = r.result
FROM _test_result r 
WHERE t.output_source_id = r.output_source_id AND t.project_id = v_imp.project_id;


-- check relation errors
SELECT json_agg(jsonb_build_object('name', format('[%s]-%s-[%s]',s.source_name, sr.relation_name, rs.source_name), 'error', COALESCE(t.result->'expression_error', t.result) ))
INTO v_err
FROM meta.relation_test t
LEFT JOIN ( meta.source_relation sr 
    JOIN meta.source s ON sr.source_id = s.source_id 
    JOIN meta.source rs ON sr.related_source_id = rs.source_id 
    ) ON sr.source_relation_id = t.source_relation_id
WHERE t.project_id = v_imp.project_id AND t.source_relation_id IS NOT NULL AND t.result IS NOT NULL AND t.result->>'data_type' IS DISTINCT FROM 'boolean';

IF v_err IS NOT NULL THEN
    PERFORM meta.svc_import_complete(in_import_id, 'F', format('Invalid relation expressions: %s', v_err));
    RETURN json_build_object('error',true);
END IF;

-- check enrichment errors
SELECT json_agg(jsonb_build_object('rule_name', e.attribute_name, 'source_name', s.source_name, 'error', COALESCE(t.result->'expression_error', t.result) ))
INTO v_err
FROM meta.enrichment_test t
LEFT JOIN meta.enrichment e ON e.enrichment_id = t.enrichment_id
LEFT JOIN meta.source s ON e.source_id = s.source_id 
WHERE t.project_id = v_imp.project_id AND t.enrichment_id IS NOT NULL AND t.result IS NOT NULL AND t.result->>'data_type' IS NULL;

IF v_err IS NOT NULL THEN
    PERFORM meta.svc_import_complete(in_import_id, 'F', format('Invalid rule expressions: %s', v_err));
    RETURN json_build_object('error',true);
END IF;


-- check output channel filter errors
SELECT json_agg(jsonb_build_object('source_name', s.source_name, 'output_name', o.output_name, 'error', COALESCE(t.result->'expression_error', t.result) ))
INTO v_err
FROM meta.output_filter_test t
LEFT JOIN meta.output_source os ON os.output_source_id = t.output_source_id
LEFT JOIN meta.source s ON os.source_id = s.source_id 
LEFT JOIN meta.output o ON o.output_id = os.output_id
WHERE t.project_id = v_imp.project_id AND t.output_source_id IS NOT NULL AND t.result IS NOT NULL AND t.result->>'data_type' IS DISTINCT FROM 'boolean';

IF v_err IS NOT NULL THEN
    PERFORM meta.svc_import_complete(in_import_id, 'F', format('Invalid output filter expressions: %s', v_err));
    RETURN json_build_object('error',true);
END IF;


-- update enrichment data types
UPDATE meta.enrichment e
SET datatype = t.result->>'data_type', datatype_schema = t.result->'schema'
FROM meta.enrichment_test t  
WHERE t.project_id = v_imp.project_id AND t.enrichment_id = e.enrichment_id AND e.datatype IS NULL;

-- generate test expressions
UPDATE meta.relation_test t
SET expression = meta.u_build_datatype_test_expr_from_parsed(sr)
FROM meta.source_relation sr  
WHERE t.source_relation_id = sr.source_relation_id AND t.project_id = v_imp.project_id AND t.expression IS NULL;

UPDATE meta.enrichment_test t
SET expression = meta.u_build_datatype_test_expr_from_parsed(e)
FROM meta.enrichment e
WHERE t.enrichment_id = e.enrichment_id AND t.project_id = v_imp.project_id AND t.expression IS NULL;

UPDATE meta.output_filter_test t
SET expression = (meta.svc_parse_enrichment(json_build_object('enrichment',json_build_object('expression', os.filter, 'source_id', os.source_id)),  
	in_mode => 'import'))->>'expression'
FROM meta.output_source os
WHERE t.output_source_id = os.output_source_id AND t.project_id = v_imp.project_id AND t.expression IS NULL;


v_next := meta.impc_test_expressions(in_import_id);

IF v_next::text = '[]' THEN   
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_imp.log_id, 'Expressions validated, Import completed successfully. ','impc_execute', 'I', clock_timestamp());
    UPDATE meta.import SET status_code = 'P' WHERE import_id = v_imp.import_id;   
    RETURN json_build_object('complete',true);
END IF;

RETURN json_build_object('next',v_next);

END;
$function$;