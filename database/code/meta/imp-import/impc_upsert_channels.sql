CREATE OR REPLACE FUNCTION meta.impc_upsert_channels(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_err jsonb;
	v_count int;
BEGIN

	CREATE TEMP TABLE _imp_channel ON COMMIT DROP AS 
	WITH ct AS (
		SELECT io.id output_id, io.name output_name, s.source_id , c.el->>'source_name' output_source_name 
			, COALESCE(c.el->>'operation_type','none') operation_type, c.el->>'filter' "filter", c.el->'mappings' mappings
			, ROW_NUMBER() OVER (PARTITION BY io.id, source_id) rn
		FROM meta.import_object io CROSS JOIN jsonb_array_elements(io.body->'channels') c(el)
		LEFT JOIN meta.source s ON s.source_name = c.el->>'source_name' AND s.project_id = in_imp.project_id
		WHERE io.import_id = in_imp.import_id 
		AND io.object_type = 'output'
	)
	SELECT output_id, output_name, source_id , output_source_name || CASE WHEN rn > 1 THEN ' ' || rn ELSE '' END output_source_name -- uniquefy channel names
		, null::int output_source_id, null::json filter_parsed, operation_type, filter, mappings
	FROM ct;

	-- check source match
	SELECT jsonb_agg(jsonb_build_object('output_name',output_name, 'source_name',output_source_name)) 
	INTO v_err
	FROM _imp_channel WHERE source_id IS NULL;

	IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','Cannot match output channel sources','error_detail', v_err);
	END IF;

	UPDATE _imp_channel c SET filter_parsed = meta.svc_parse_enrichment(json_build_object('enrichment',json_build_object('expression', c.filter, 'source_id', c.source_id)),  
	in_mode => 'import')
	WHERE c.filter IS NOT NULL;

	-- check errors
	SELECT jsonb_agg(jsonb_build_object('source_name', c.output_source_name, 'output_name', c.output_name, 'error', c.filter_parsed->>'error'))
	INTO v_err
	FROM _imp_channel c 
	WHERE c.filter_parsed->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Filter expression parsing errors','error_detail',v_err);
	END IF;

	-- update id
	UPDATE _imp_channel SET output_source_id = nextval('meta.seq_output_source'::regclass)::int;

	-- insert channels
	INSERT INTO meta.output_source(output_source_id, source_id ,output_id ,filter ,operation_type ,active_flag ,created_userid ,create_datetime ,output_package_parameters ,output_source_name ,include_pass_flag ,include_fail_flag ,include_warn_flag ,description )
	SELECT c.output_source_id, c.source_id, c.output_id, replace(c.filter, '[This].', ''), c.operation_type, COALESCE(j.active_flag,true), 'Import ' || in_imp.import_id, 
	in_imp.create_datetime, j.output_package_parameters, c.output_source_name, COALESCE(j.include_pass_flag,true), COALESCE(j.include_fail_flag,false), COALESCE(j.include_warn_flag,false), j.description
	FROM _imp_channel c
	JOIN meta.output o ON o.output_id = c.output_id
	CROSS JOIN jsonb_populate_record(null::meta.output_source, to_jsonb(c)) j;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Inserted %s channels', v_count),'svc_import_execute', 'I', clock_timestamp());


	-- build mapping table
	CREATE TEMP TABLE _imp_mapping ON COMMIT DROP AS	
	SELECT c.output_source_id, mapping
	FROM _imp_channel c 
	CROSS JOIN jsonb_array_elements_text(c.mappings) el
    CROSS JOIN meta.impc_parse_mapping(to_jsonb(c), el) mapping;

	-- check columns mapping
	SELECT jsonb_agg(mapping->'error') 
	INTO v_err
	FROM _imp_mapping WHERE mapping->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN  jsonb_build_object('error','Mapping errors','error_detail',v_err);
	END IF;

	INSERT INTO meta.output_source_column(output_source_id ,output_column_id ,expression ,datatype ,
	type ,enrichment_id ,raw_attribute_id ,system_attribute_id , source_relation_ids ,create_datetime ,created_userid, aggregate ,aggregate_distinct_flag, keys )
	SELECT m.output_source_id ,j.output_column_id ,j.expression ,j.datatype ,
	j.type ,j.enrichment_id ,j.raw_attribute_id ,j.system_attribute_id , null ,in_imp.create_datetime, 'Import ' || in_imp.import_id, j.aggregate ,j.aggregate_distinct_flag, j.keys 
	FROM _imp_mapping m
	CROSS JOIN jsonb_populate_record(null::meta.output_source_column, m.mapping) j;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Inserted %s column mappings', v_count),'svc_import_execute', 'I', clock_timestamp());

-- save test expressions
	DELETE FROM meta.output_filter_test WHERE project_id = in_imp.project_id;
-- save test expressions for filter expressions that can be resolved (point to raw + system attributes)
	INSERT INTO meta.output_filter_test (output_source_id, project_id, expression)
	SELECT c.output_source_id, in_imp.project_id, c.filter_parsed->>'expression'
	FROM _imp_channel c 
	WHERE c.filter IS NOT NULL; 	

	RETURN NULL;	
				  
END;
$function$;