CREATE OR REPLACE FUNCTION meta.impc_upsert_enrichment_parameters(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_err jsonb;
BEGIN

    -- create relation mapping table
    CREATE TEMP TABLE _imp_relation ON COMMIT DROP AS
    SELECT sr.source_relation_id, format('[%s]-%s-[%s]', s.source_name, sr.relation_name, rs.source_name) relation_uid
    FROM meta.source_relation sr
    JOIN meta.source s ON sr.source_id = s.source_id AND s.project_id = in_imp.project_id
    JOIN meta.source rs ON sr.related_source_id = rs.source_id 
    WHERE sr.active_flag;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Created relation mapping table','svc_import_execute', 'I', clock_timestamp());

	-- check for invalid relations in rules
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'relation', rel))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	CROSS JOIN jsonb_array_elements_text(p->'relations') rel 
    LEFT JOIN _imp_relation r ON r.relation_uid = rel
	LEFT JOIN meta.source s ON s.source_id = e.source_id
	WHERE r.relation_uid IS NULL;

    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Invalid relation paths in rule parameters','error_detail',v_err);
	END IF;

	-- check for invalid source names in rule parameters
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'parameter_source_name', p->>'source_name'))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	LEFT JOIN meta.source s ON s.source_id = e.source_id
    LEFT JOIN meta.source ps ON ps.source_name = p->>'source_name' AND ps.project_id = in_imp.project_id
	WHERE p->>'source_name' IS NOT NULL AND ps.source_id IS NULL;

    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Invalid source names in rule parameters','error_detail',v_err);
	END IF;

    -- check for duplicate source names in rule parameters
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'parameter_source_name', p->>'source_name'))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	LEFT JOIN meta.source s ON s.source_id = e.source_id
    GROUP BY s.source_name, e.attribute_name, p->>'source_name' HAVING COUNT(1) > 1;


    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Duplicate relation paths in rule parameters','error_detail',v_err);
	END IF;

    -- check for blank source names in rule parameters
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name))
	INTO v_err
	FROM _imp_enrichment e 
	CROSS JOIN jsonb_array_elements(e.rule->'parameters') p
	LEFT JOIN meta.source s ON s.source_id = e.source_id
    WHERE p->>'source_name' IS NULL
    AND (SELECT COUNT(DISTINCT m[1])
        FROM regexp_matches(e.rule->>'expression', '\[([^\]]+)].\w+','g') m
        WHERE m[1] !~ 'This|\d+' -- exclude This and array indexes
    ) > 1;

    IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','When rule expression references multiple sources, rule parameter is required to have `source_name`','error_detail',v_err);
	END IF;

	-- build enrichment parameter table
    CREATE TEMP TABLE _impc_enrichment_parameter ON COMMIT DROP AS 
    SELECT e.enrichment_id,  p->>'source_name' source_name, meta.imp_map_relations(p->'relations') source_relation_ids
    FROM _imp_enrichment e
    CROSS JOIN jsonb_array_elements(e.rule->'parameters') p;


	-- parse enrichments
    CREATE TEMP TABLE _impc_enrichment_parsed ON COMMIT DROP AS 
	SELECT e.enrichment_id, p->>'expression' expression, p->'enrichment' enrichment, p->'params' params, p->'aggs' aggs, p->>'error' error
	FROM _imp_enrichment ie
	JOIN meta.enrichment e ON e.enrichment_id = ie.enrichment_id
	CROSS JOIN meta.svc_parse_enrichment(json_build_object('enrichment',e), in_mode => 'import') p; -- save test expressions for rule expressions that can be resolved (point to raw + system attributes)

	-- check errors
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 'error', ep.error))
	INTO v_err
	FROM _impc_enrichment_parsed ep
	JOIN _imp_enrichment e ON ep.enrichment_id = e.enrichment_id
	LEFT JOIN meta.source s ON s.source_id = e.source_id
	WHERE ep.error IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Rule expression parsing errors','error_detail',v_err);
	END IF;

	-- check parameter errors
	SELECT jsonb_agg(jsonb_build_object('source_name', s.source_name, 'rule_name', e.attribute_name, 
	'parameter', format('[%s].%s', p->>'source_name', p->>'attribute_name'), 'error', p->'paths'->>'error'))
	INTO v_err
	FROM _impc_enrichment_parsed ep
	JOIN _imp_enrichment e ON ep.enrichment_id = e.enrichment_id
	LEFT JOIN meta.source s ON s.source_id = e.source_id
	CROSS JOIN json_array_elements(ep.params) p
	WHERE p->'paths'->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Rule relations errors','error_detail',v_err);
	END IF;

	-- save parameters
	INSERT INTO meta.enrichment_parameter(enrichment_parameter_id, parent_enrichment_id, type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
	source_relation_ids, self_relation_container, create_datetime, aggregation_id)
	SELECT (par->>'id')::int, ep.enrichment_id, p.type, p.enrichment_id, p.raw_attribute_id, p.system_attribute_id, p.source_id, 
    p.source_relation_ids, CASE WHEN par->>'source_name' <> 'This' AND e.source_id = p.source_id THEN 'Related' END self_relation_container, now(), p.aggregation_id
	FROM _impc_enrichment_parsed ep 
	JOIN _imp_enrichment e ON ep.enrichment_id = e.enrichment_id
	CROSS JOIN json_array_elements(ep.params) par
	CROSS JOIN json_populate_record(null::meta.enrichment_parameter,par) p;

	-- save aggregates
	INSERT INTO meta.enrichment_aggregation(enrichment_aggregation_id, enrichment_id, expression, function, relation_ids, create_datetime)
	SELECT (a->>'id')::int, ep.enrichment_id, a->>'expression_parsed', a->>'function', meta.u_json_array_to_int_array(a->'relation_ids'), now() 
	FROM _impc_enrichment_parsed ep 
	CROSS JOIN json_array_elements(ep.aggs) a;

	-- save test expressions
	DELETE FROM meta.enrichment_test WHERE project_id = in_imp.project_id;

	INSERT INTO meta.enrichment_test (enrichment_id, project_id, expression)
	SELECT enrichment_id, in_imp.project_id, expression
	FROM _impc_enrichment_parsed ep; 

	-- update expression_parsed
	UPDATE meta.enrichment e
	SET expression_parsed = ep.enrichment->>'expression_parsed'
	FROM _impc_enrichment_parsed ep
	WHERE e.enrichment_id = ep.enrichment_id;

	RETURN null;
	  
END;
$function$;