CREATE OR REPLACE FUNCTION meta.impc_upsert_raw_attributes(in_imp meta.import)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_count int;
	v_err jsonb;
BEGIN

    -- raw attributes temp table
    CREATE TEMP TABLE _imp_raw_attribute ON COMMIT DROP AS
	WITH ct AS (
    	SELECT io.id source_id, io.body->>'source_name' source_name, meta.impc_parse_raw_attribute(ra) raw_parsed
		FROM meta.import_object io 
		CROSS JOIN jsonb_array_elements(io.body->'raw_attributes') ra
		WHERE io.import_id = in_imp.import_id 
        AND io.object_type = 'source'
	)
	SELECT ra.raw_attribute_id, ct.source_id, ct.source_name, raw_parsed
    FROM ct
	LEFT JOIN meta.raw_attribute ra ON ra.source_id = ct.source_id AND ra.column_alias = raw_parsed->>'name';

	-- check parse errors
	SELECT jsonb_agg(jsonb_build_object('source_name', source_name, 'error', raw_parsed->'error'))
	INTO v_err
	FROM _imp_raw_attribute r 
	WHERE raw_parsed->>'error' IS NOT NULL;

	IF v_err IS NOT NULL THEN
		RETURN v_err;
	END IF;

	UPDATE _imp_raw_attribute SET raw_attribute_id = nextval('meta.seq_raw_attribute'::regclass)::int
	WHERE raw_attribute_id IS NULL;

	--upsert raw attributes
	INSERT INTO meta.raw_attribute(raw_attribute_id, source_id ,raw_attribute_name ,column_normalized ,data_type, datatype_schema, version_number ,
	column_alias ,unique_flag ,update_datetime ,updated_userid)
	SELECT j.raw_attribute_id, j.source_id, j.raw_parsed->>'name',  
	j.raw_parsed->>'name', j.raw_parsed->>'data_type',j.raw_parsed->'datatype_schema'
	,1 ,j.raw_parsed->>'name', false, in_imp.create_datetime, 'Import ' || in_imp.import_id
	FROM _imp_raw_attribute j
    ON CONFLICT (raw_attribute_id)
     DO UPDATE SET 
		raw_attribute_name = EXCLUDED.raw_attribute_name,
		column_normalized = EXCLUDED.column_normalized,
		data_type = EXCLUDED.data_type,
		datatype_schema = EXCLUDED.datatype_schema,
		update_datetime = in_imp.create_datetime,
		updated_userid = 'Import ' || in_imp.import_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_imp.log_id, format('Upserted %s raw attributes', v_count),'imp_upsert_raw_attributes_core', 'I', clock_timestamp());

	
	-- delete raw attributes not in the import file
	DELETE FROM meta.raw_attribute ra
	WHERE ra.raw_attribute_id IN (SELECT r.raw_attribute_id 
			FROM meta.raw_attribute r 
			JOIN meta.source s ON s.source_id = r.source_id AND s.project_id = in_imp.project_id
			LEFT JOIN _imp_raw_attribute ir ON ir.raw_attribute_id = r.raw_attribute_id
			WHERE ir.raw_attribute_id IS NULL
			);

	GET DIAGNOSTICS v_count = ROW_COUNT;
	INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
	VALUES ( in_imp.log_id, format('Deleted %s raw attributes', v_count),'imp_upsert_raw_attributes_core', 'I', clock_timestamp());

	RETURN null;

END;
$function$;