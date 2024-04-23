CREATE OR REPLACE FUNCTION meta.impc_upsert_output(in_o meta.import_object, in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_id int := in_o.id;
	v_error text;	
	v_body jsonb;
BEGIN

IF in_o.body->>'table_name' IS NULL THEN
	RETURN jsonb_build_object('error', 'table_name is undefined');
END IF;

v_body := in_o.body || 
	jsonb_build_object('output_package_parameters',jsonb_build_object('table_name', COALESCE(in_o.body->>'table_name',in_o.name),
																	  'table_schema', in_o.body->>'schema_name') ) ;

IF v_id IS NULL THEN
	INSERT INTO meta.output(output_type ,output_name ,active_flag ,created_userid ,create_datetime ,output_package_parameters ,retention_parameters ,output_description ,output_sub_type,
	 alert_parameters ,post_output_type , project_id)
	SELECT COALESCE(j.output_type,'table'), j.output_name, COALESCE(j.active_flag, true), 'Import ' || in_imp.import_id, in_imp.create_datetime, 
	j.output_package_parameters, j.retention_parameters, j.output_description, j.output_sub_type, 
	j.alert_parameters, COALESCE(j.post_output_type,'none'), in_imp.project_id
	FROM jsonb_populate_record(null::meta.output, v_body ) j
	RETURNING output_id INTO v_id;
ELSE

	SELECT meta.u_append_object(to_jsonb(o),v_body) 
	INTO v_body
	FROM meta.output o
	WHERE o.output_id = v_id;

	UPDATE meta.output t
	SET	output_type = COALESCE(j.output_type,'table'),
		output_name = j.output_name,
		active_flag = COALESCE(j.active_flag, true),
		update_datetime = in_imp.create_datetime,
		output_package_parameters = j.output_package_parameters,
		updated_userid = 'Import ' || in_imp.import_id,
		retention_parameters = j.retention_parameters,
		output_description = j.output_description,
		output_sub_type = j.output_sub_type,
		alert_parameters = j.alert_parameters,
		post_output_type = COALESCE(j.post_output_type,'none')
	FROM jsonb_populate_record(null::meta.output, v_body) j 
	WHERE t.output_id = v_id;

	--delete/insert columns TODO: compare columns & only update different

		DELETE FROM meta.output_source_column osc
		USING meta.output_source os
		WHERE os.output_id = v_id AND os.output_source_id = osc.output_source_id;
		
		DELETE FROM meta.output_column oc
		WHERE oc.output_id = v_id;

		DELETE FROM meta.output_source os
		WHERE os.output_id = v_id;

END IF;


-- parse & insert columns
INSERT INTO meta.output_column(output_id ,position ,name ,datatype  ,created_userid ,create_datetime)
SELECT v_id, el.idx, col[1] name, COALESCE(col[2],'string') datatype,  'Import ' || in_imp.import_id, in_imp.create_datetime
FROM jsonb_array_elements_text(in_o.body->'columns') WITH ORDINALITY  el(js, idx)
CROSS JOIN regexp_split_to_array(js, '\s+') col;

-- TODO: add datatype validation when we add create table DDL

SELECT meta.u_validate_output(o) INTO v_error
FROM meta.output o
WHERE o.output_id = v_id;

IF v_error != '' THEN
	RETURN jsonb_build_object('output_name',in_o.name,'error', v_error);
END IF;

RETURN null;
END;
$function$;