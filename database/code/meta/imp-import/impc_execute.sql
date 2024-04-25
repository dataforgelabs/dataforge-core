CREATE OR REPLACE FUNCTION meta.impc_execute(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_err jsonb;
    v_count int;
BEGIN
     -- upsert changed objects in the order to maintain ref integrity
    -- update source_ids
    UPDATE meta.import_object io 
    SET id = s.source_id 
    FROM meta.source s
        WHERE io.import_id = in_imp.import_id AND s.project_id = in_imp.project_id
        AND s.source_name = io.name
        AND io.object_type = 'source';

    -- upsert sources part 1 and update ids in import_object table
    WITH us AS (
        SELECT meta.impc_upsert_source(io, in_imp) s
        FROM meta.import_object io 
        WHERE io.import_id = in_imp.import_id 
        AND io.object_type = 'source'
    )    
    SELECT jsonb_agg(s)
    INTO v_err
    FROM us
    WHERE s IS NOT NULL;

    IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Imported %s sources',v_count ),'impc_execute', 'I', clock_timestamp());

    UPDATE meta.import_object io
    SET id = s.source_id
    FROM meta.source s 
    WHERE io.import_id = in_imp.import_id AND io.id IS NULL AND io.object_type = 'source' AND io.name = s.source_name AND s.project_id = in_imp.project_id;

    -- delete all enr parameters in project
    DELETE FROM meta.enrichment_parameter ep
    WHERE ep.parent_enrichment_id IN (SELECT enrichment_id FROM meta.enrichment e 
            JOIN meta.source s ON s.source_id = e.source_id
            WHERE s.project_id = in_imp.project_id);

    -- delete all enr aggs in project
    DELETE FROM meta.enrichment_aggregation ea
    WHERE ea.enrichment_id IN (SELECT enrichment_id FROM meta.enrichment e 
            JOIN meta.source s ON s.source_id = e.source_id
            WHERE s.project_id = in_imp.project_id);

   -- delete relation parameters
    DELETE FROM meta.source_relation_parameter
    WHERE source_relation_id IN (SELECT source_relation_id FROM meta.source_relation sr
            JOIN meta.source s ON s.source_id = sr.source_id
            WHERE s.project_id = in_imp.project_id);

    -- delete mappings
    DELETE FROM meta.output_source_column osc
    WHERE osc.output_source_id IN (SELECT os.output_source_id FROM meta.output_source os JOIN meta.source s ON s.source_id = os.source_id
            WHERE s.project_id = in_imp.project_id);

    v_err = meta.impc_upsert_raw_attributes(in_imp);

    IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','raw attribute','error_detail', v_err);
	END IF;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Imported raw attributes','svc_import_execute', 'I', clock_timestamp());

    -- create enrichment rules temp table
	CREATE TEMP TABLE _imp_enrichment ON COMMIT DROP AS 
	SELECT io.id source_id, io.name source_name, e.enrichment_id, r.rule, null::int parent_enrichment_id, r.rule->>'name' attribute_name
    FROM meta.import_object io CROSS JOIN jsonb_array_elements(io.body->'rules') r (rule)
	LEFT JOIN meta.enrichment e ON e.source_id = io.id AND e.attribute_name = r.rule->>'name' AND e.active_flag = COALESCE((r.rule->>'active_flag')::boolean, true)
    WHERE io.import_id = in_imp.import_id 
    AND io.object_type = 'source';

    SELECT jsonb_agg(jsonb_build_object('source_name',source_name,'attribute_name',attribute_name))
    INTO v_err
    FROM (SELECT source_name, rule->>'name' attribute_name
        FROM _imp_enrichment 
        GROUP BY source_name, rule->>'name' HAVING COUNT(1) > 1) dupes;
    
    IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','Duplicate rules','error_detail', v_err);
	END IF;

    SELECT jsonb_agg(jsonb_build_object('source_name',source_name,'attribute_name',rule->>'name'))
    INTO v_err
    FROM _imp_enrichment
    WHERE NOT rule->>'name' ~ '^[a-z_]+[a-z0-9_]*$';

    IF v_err IS NOT NULL THEN
		RETURN jsonb_build_object('error','Invalid rule name(s). Name has to start with lowercase letter or _ It may contain lowercase letters, numbers and _',
        'error_detail', v_err);
	END IF;

    UPDATE _imp_enrichment
    SET enrichment_id = nextval('meta.seq_enrichment'::regclass)::int
    WHERE enrichment_id IS NULL;

    PERFORM meta.impc_upsert_enrichments(in_imp);

    --upsert relations, relation parameters
    v_err := meta.impc_upsert_relations(in_imp);
    IF v_err IS NOT NULL THEN
		RETURN  v_err;
	END IF;

    -- upsert enrichment parameters
    v_err :=  meta.impc_upsert_enrichment_parameters(in_imp);
    IF v_err IS NOT NULL THEN
		RETURN  v_err;
	END IF;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Imported enrichment parameters','impc_execute', 'I', clock_timestamp());

    -- update output ids
    UPDATE meta.import_object io 
    SET id = o.output_id
    FROM meta.output o
    WHERE io.import_id = in_imp.import_id AND io.id IS NULL AND io.object_type = 'output' AND io.name = o.output_name AND o.project_id = in_imp.project_id;

    -- upsert outputs
    WITH uo AS (
        SELECT meta.impc_upsert_output(io, in_imp) o
        FROM meta.import_object io 
        WHERE io.import_id = in_imp.import_id 
        AND io.object_type = 'output'
    )
    SELECT json_agg(o)
    INTO v_err
    FROM uo
    WHERE o IS NOT NULL;

    IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Imported %s outputs',v_count),'impc_execute', 'I', clock_timestamp());

    UPDATE meta.import_object io
    SET id = o.output_id
    FROM meta.output o
    WHERE io.import_id = in_imp.import_id AND io.id IS NULL AND io.object_type = 'output' AND io.name = o.output_name AND o.project_id = in_imp.project_id;

    -- upsert channels
    v_err := meta.impc_upsert_channels(in_imp);

    IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

    -- cascade delete objects not existing in import
    PERFORM meta.u_delete_cascade(source_id, 'source')
    FROM meta.source s
    WHERE s.project_id = in_imp.project_id
    AND s.source_id NOT IN (SELECT id FROM meta.import_object io
            WHERE io.import_id = in_imp.import_id AND io.object_type = 'source');


    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
    VALUES ( in_imp.log_id, format('Deleted %s sources not existing in import',v_count),'impc_execute', 'I', clock_timestamp());

    PERFORM meta.u_delete_cascade(output_id, 'output')
    FROM meta.output o
    WHERE o.project_id = in_imp.project_id
    AND o.output_id NOT IN (SELECT id FROM meta.import_object io
            WHERE io.import_id = in_imp.import_id AND io.object_type = 'output');


    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
    VALUES ( in_imp.log_id, format('Deleted %s outputs not existing in import',v_count),'impc_execute', 'I', clock_timestamp());


    -- validate all imported enrichment parameters
    WITH c AS (
        SELECT e.enrichment_id, e.attribute_name, e.source_id, meta.u_validate_expression_parameters(e) val
        FROM meta.enrichment e JOIN _imp_enrichment ie ON e.enrichment_id = ie.enrichment_id
    )
    SELECT jsonb_agg(jsonb_build_object('attribute_name',c.attribute_name, 'source_name',s.source_name, 'error', val))
        INTO v_err 
    FROM c JOIN meta.source s ON s.source_id = c.source_id
    WHERE val IS DISTINCT FROM '';
    
    IF v_err IS NOT NULL THEN 
        RETURN jsonb_build_object('error' , 'Rule validation errors', 'error_detail', v_err );
    END IF;
    
    -- validate all imported output mappings
    WITH c AS (
        SELECT meta.u_validate_output_mapping(om) val
        FROM  meta.output_source_column om 
	    JOIN _imp_mapping im ON om.output_source_id = im.output_source_id 
    )
    SELECT jsonb_agg(val)
        INTO v_err 
    FROM c 
    WHERE val IS DISTINCT FROM '';
    
    IF v_err IS NOT NULL THEN 
        RETURN jsonb_build_object('error' , 'Output mapping validation errors', 'error_detail', v_err );
    END IF;
    
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, 'Import files parsed successfully. ','impc_execute', 'I', clock_timestamp());

    RETURN null;    
END;
$function$;
