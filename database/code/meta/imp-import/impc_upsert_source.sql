CREATE OR REPLACE FUNCTION meta.impc_upsert_source(in_o meta.import_object, in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_id int := in_o.id;
	v_hub_view_name text;	
	v_body jsonb;
BEGIN

IF in_o.body->>'source_table' IS NULL THEN
	RETURN jsonb_build_object('error', format('source_table is undefined for source `%s`',in_o.body->>'source_name'));
END IF;

v_hub_view_name :=  COALESCE(in_o.body->>'target_table',in_o.name); 
v_body := in_o.body || jsonb_build_object('ingestion_parameters',jsonb_build_object('source_query', in_o.body->>'source_query',
		'source_table', in_o.body->>'source_table'));

IF in_o.id IS NULL THEN
	INSERT INTO meta.source(source_name ,source_description ,active_flag , ingestion_parameters, create_datetime, created_userid, parsing_parameters ,cdc_refresh_parameters ,alert_parameters ,file_type ,refresh_type ,
	connection_type ,initiation_type ,cost_parameters ,parser ,	hub_view_name , project_id)
	SELECT j.source_name, COALESCE(v_body->>'description',''), COALESCE(j.active_flag,true), j.ingestion_parameters, in_imp.create_datetime, 'Import ' || in_imp.import_id created_userid, j.parsing_parameters, j.cdc_refresh_parameters, j.alert_parameters, j.file_type, COALESCE(j.refresh_type,'full'), 
	j.connection_type, j.initiation_type, j.cost_parameters, j.parser, v_hub_view_name, in_imp.project_id
	FROM jsonb_populate_record(null::meta.source, v_body) j
	RETURNING source_id INTO v_id;

ELSE
	-- Get current source json
	-- Update all attributes to values in import file, then append all default values
	-- Do not update existing attributes not present in import

	SELECT meta.u_append_object(to_jsonb(s),v_body) 
	INTO v_body
	FROM meta.source s
	WHERE s.source_id = in_o.id;

	UPDATE meta.source t
	SET	source_name = j.source_name, 
		source_description = COALESCE(j.source_description,''),
		active_flag = COALESCE(j.active_flag, true),
		ingestion_parameters = j.ingestion_parameters,
		update_datetime = in_imp.create_datetime,
		parsing_parameters = j.parsing_parameters,
		cdc_refresh_parameters = j.cdc_refresh_parameters,
		updated_userid = 'Import ' || in_imp.import_id,
		alert_parameters = j.alert_parameters,
		file_type = j.file_type,
		refresh_type = j.refresh_type,
		connection_type = j.connection_type,
		initiation_type = j.initiation_type,
		cost_parameters = j.cost_parameters,
		parser = j.parser,
		hub_view_name = v_hub_view_name
	FROM jsonb_populate_record(null::meta.source,  v_body) j 
	WHERE t.source_id = in_o.id;
END IF;
			  
RETURN null;	
END;
$function$;