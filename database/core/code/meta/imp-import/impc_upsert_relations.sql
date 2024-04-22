CREATE OR REPLACE FUNCTION meta.impc_upsert_relations(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_err jsonb;
    v_count int;
BEGIN
    -- create relation temp table
    CREATE TEMP TABLE _imp_relation ON COMMIT DROP AS
    WITH ir AS (
        SELECT j.relation_name, j.source_id, j.related_source_id, j.expression, j.source_cardinality, j.related_source_cardinality, j.expression_parsed, 
        COALESCE(j.active_flag, true) active_flag, j.description, COALESCE(j.primary_flag, true) primary_flag, el rel, rel_js->'error' error
        FROM meta.import_object o 
        CROSS JOIN jsonb_array_elements(COALESCE(NULLIF(o.body,'null'),'[]'::jsonb)) el
        CROSS JOIN meta.imp_decode_relation(el,in_imp.project_id) rel_js
        CROSS JOIN jsonb_populate_record(null::meta.source_relation, rel_js) j
        WHERE o.object_type = 'relations' AND o.import_id = in_imp.import_id
    )
     SELECT sr.source_relation_id, ir.*, null::jsonb rel_parsed
     FROM ir LEFT JOIN meta.source_relation sr ON ir.source_id = sr.source_id AND ir.related_source_id = sr.related_source_id AND ir.relation_name = sr.relation_name;


	-- check decode errors
    SELECT jsonb_agg(jsonb_build_object('name', r.rel->>'name', 'error', r.error))
    INTO v_err
    FROM _imp_relation r
    WHERE r.error IS NOT NULL;

	IF v_err IS NOT NULL THEN
        RETURN jsonb_build_object('error','Relation error(s)','error_detail',v_err);
    END IF;

	-- check duplicates
    WITH dup AS (
        SELECT r.source_id, r.relation_name, r.related_source_id 
		FROM _imp_relation r 
        GROUP BY r.source_id, r.relation_name, r.related_source_id HAVING COUNT(1) > 1   
        )
    SELECT jsonb_agg(jsonb_build_object('name', format('[%s]-%s-[%s]',s.source_name, r.relation_name, rs.source_name)))
    INTO v_err
    FROM dup r 
    LEFT JOIN meta.source s ON s.source_id = r.source_id
    LEFT JOIN meta.source rs ON s.source_id = r.related_source_id;

	IF v_err IS NOT NULL THEN
        RETURN jsonb_build_object('error','Duplicate relations','error_detail',v_err);
    END IF;

    -- delete relations not present in import file
    DELETE FROM meta.source_relation sr
    WHERE sr.source_id IN (SELECT source_id FROM meta.source s WHERE s.project_id = in_imp.project_id)
    AND sr.source_relation_id NOT IN (SELECT source_relation_id FROM _imp_relation);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Deleted %s relations', v_count),'svc_import_execute', 'I', clock_timestamp());

    UPDATE _imp_relation SET source_relation_id = nextval('meta.seq_source_relation'::regclass)::int WHERE source_relation_id IS NULL;

    -- insert new relations
    INSERT INTO meta.source_relation(source_relation_id, relation_name ,source_id ,related_source_id ,expression ,source_cardinality ,related_source_cardinality ,expression_parsed ,active_flag ,create_datetime ,created_userid ,description ,primary_flag)
    SELECT source_relation_id, relation_name ,source_id ,related_source_id ,expression ,source_cardinality ,related_source_cardinality ,expression_parsed ,active_flag ,in_imp.create_datetime, 'Import ' || in_imp.import_id created_userid, description ,primary_flag 
    FROM _imp_relation
    ON CONFLICT (source_relation_id) DO UPDATE
    SET relation_name = EXCLUDED.relation_name, 
        source_id = EXCLUDED.source_id, 
        related_source_id = EXCLUDED.related_source_id ,
        expression = EXCLUDED.expression, 
        source_cardinality = EXCLUDED.source_cardinality ,
        related_source_cardinality = EXCLUDED.related_source_cardinality, 
        active_flag = EXCLUDED.active_flag, 
        update_datetime = in_imp.create_datetime, 
        updated_userid = 'Import ' || in_imp.import_id, 
        description = EXCLUDED.description, 
        primary_flag = EXCLUDED.primary_flag;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Upserted %s relations', v_count),'svc_import_execute', 'I', clock_timestamp());

    -- parse expressions
    UPDATE _imp_relation ir
    SET rel_parsed = meta.impc_parse_relation(to_jsonb(ir));

	-- check parse errors
    SELECT jsonb_agg(jsonb_build_object('name', r.rel->>'name', 'expression', r.expression, 'error', rel_parsed->>'error'))
    INTO v_err
    FROM _imp_relation r
    WHERE rel_parsed->>'error' IS NOT NULL OR rel_parsed->>'expression_parsed' IS NULL;

	IF v_err IS NOT NULL THEN
        RETURN jsonb_build_object('error','Relation expression parse error(s)','error_detail',v_err);
    END IF;

    UPDATE meta.source_relation sr
    SET expression_parsed = ir.rel_parsed->>'expression_parsed'
    FROM _imp_relation ir 
    WHERE ir.source_relation_id = sr.source_relation_id;

    -- save relations for testing
    DELETE FROM meta.relation_test WHERE project_id = in_imp.project_id;

    INSERT INTO meta.relation_test (source_relation_id, project_id, expression)
    SELECT ir.source_relation_id, in_imp.project_id, ir.rel_parsed->>'test_expression'
    FROM _imp_relation ir;
     
    DROP TABLE _imp_relation;

    RETURN null;
END;
$function$;