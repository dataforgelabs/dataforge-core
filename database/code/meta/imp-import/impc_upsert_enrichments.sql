CREATE OR REPLACE FUNCTION meta.impc_upsert_enrichments(in_imp meta.import)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_count int;
BEGIN
    -- delete rules that don't exist in import

    DELETE FROM meta.enrichment e 
    WHERE e.source_id IN (SELECT s.source_id FROM meta.source s WHERE s.project_id = in_imp.project_id)
    AND e.enrichment_id NOT IN (SELECT enrichment_id FROM  _imp_enrichment );

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Deleted %s enrichments', v_count),'imp_upsert_enrichments', 'I', clock_timestamp());

    --INSERT changed enrichments (includes new)
	INSERT INTO meta.enrichment(enrichment_id, parent_enrichment_id ,source_id ,name ,description, cast_datatype ,attribute_name ,expression ,rule_type_code ,validation_action_code ,validation_type_code ,keep_current_flag ,window_function_flag ,unique_flag ,active_flag ,create_datetime ,created_userid)
	SELECT ie.enrichment_id, ie.parent_enrichment_id, ie.source_id,  j.name, j.description, COALESCE(j.cast_datatype,''), COALESCE(j.attribute_name,j.name) attribute_name, j.expression, COALESCE(j.rule_type_code,'E'), j.validation_action_code, j.validation_type_code, COALESCE(j.keep_current_flag, j.expression ~* 'over\s*\(.*\)'), COALESCE(j.window_function_flag,false), COALESCE(j.unique_flag,false), COALESCE(j.active_flag,true), in_imp.create_datetime, 'Import ' || in_imp.import_id
	FROM _imp_enrichment ie CROSS JOIN jsonb_populate_record(null::meta.enrichment, ie.rule) j
    ON CONFLICT (enrichment_id) DO UPDATE
    SET parent_enrichment_id = EXCLUDED.parent_enrichment_id, 
    source_id = EXCLUDED.source_id, 
    name = EXCLUDED.name, 
    description = EXCLUDED.description, 
    cast_datatype = EXCLUDED.cast_datatype, 
    attribute_name = EXCLUDED.attribute_name, 
    expression = EXCLUDED.expression, 
    rule_type_code = EXCLUDED.rule_type_code, 
    validation_action_code = EXCLUDED.validation_action_code, 
    validation_type_code = EXCLUDED.validation_type_code, 
    keep_current_flag = EXCLUDED.keep_current_flag, 
    window_function_flag = EXCLUDED.window_function_flag,
    unique_flag = EXCLUDED.unique_flag, 
    active_flag = EXCLUDED.active_flag, 
    update_datetime = in_imp.create_datetime, 
    updated_userid = 'Import ' || in_imp.import_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Upserted %s enrichments', v_count),'svc_import_execute', 'I', clock_timestamp());

END;
$function$;