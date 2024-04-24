CREATE OR REPLACE FUNCTION meta.svc_generate_queries(in_import_id int)
 RETURNS json 
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_imp meta.import;
    v_source_queries json;
    v_output_queries json;
	v_level int;
	v_count int;
	v_err json;
	v_all_source_query text;
	v_all_output_query text;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

	CREATE TEMP TABLE _sources ON COMMIT DROP AS
	SELECT s.source_id, s.source_name, 0 as level , null::text file_name , null::text query
	FROM meta.source s
	WHERE s.project_id = v_imp.project_id AND
	NOT EXISTS(SELECT 1 FROM meta.enrichment e JOIN meta.enrichment_parameter ep ON e.enrichment_id = ep.parent_enrichment_id
					WHERE e.source_id = s.source_id AND ep.source_id <> s.source_id);


	FOR v_level IN 1..20 LOOP
		INSERT INTO _sources (source_id, source_name, level)
		SELECT s.source_id, s.source_name, v_level
		FROM meta.source s
		WHERE s.project_id = v_imp.project_id AND
		s.source_id NOT IN (SELECT source_id FROM _sources) AND
		NOT EXISTS(SELECT 1 
			FROM meta.enrichment e 
			JOIN meta.enrichment_parameter ep ON e.enrichment_id = ep.parent_enrichment_id
			JOIN meta.source_relation sr ON sr.source_relation_id = ANY(ep.source_relation_ids)
			WHERE e.source_id = s.source_id 
			AND s.source_id NOT IN (sr.source_id, sr.related_source_id) 
			AND	NOT EXISTS(SELECT 1 FROM _sources _s WHERE _s.source_id = sr.source_id) 
			AND	NOT EXISTS(SELECT 1 FROM _sources _s WHERE _s.source_id = sr.related_source_id) 
		);
		GET DIAGNOSTICS v_count = ROW_COUNT;
		EXIT WHEN v_count = 0;
	END LOOP;

	SELECT json_agg(s.source_name) 
	INTO v_err
	FROM meta.source s
	WHERE s.project_id = v_imp.project_id 
	AND s.source_id NOT IN (SELECT source_id FROM _sources);

	IF v_err IS NOT NULL THEN
		RETURN json_build_object('error',format('Circular dependencies in sources %s. Please check rules to ensure that 2 sources do not have lookups pointing to each other',v_err));
	END IF;

	-- clean file names
	UPDATE _sources s
	SET file_name = lower(regexp_replace(left(s.source_name,245),'["<>:\/\\|?*]','_','g')),
		query = meta.u_enr_query_generate_query(s.source_id);

	WITH w AS (
		SELECT s.source_id, ROW_NUMBER() OVER (PARTITION BY s.file_name ORDER BY s.source_id) rn
	    FROM _sources s
	)
	UPDATE _sources s
	SET file_name = file_name || '_' || w.rn
	FROM w
	WHERE w.source_id = s.source_id AND w.rn > 1;

	SELECT json_agg(json_build_object('file_name', file_name || '.sql', 'query', query ))
	INTO v_source_queries
	FROM _sources;

	SELECT string_agg(query,E'\n\n' ORDER BY level)
	INTO v_all_source_query
	FROM _sources;

	CREATE TEMP TABLE _outputs ON COMMIT DROP AS
	SELECT o.output_id, o.output_name, 0 as level , null::text file_name , null::text query
	FROM meta.output o WHERE o.project_id = v_imp.project_id;

	-- clean file names
	UPDATE _outputs o
	SET file_name = lower(regexp_replace(left(o.output_name,245),'["<>:\/\\|?*]','_','g')),
	query = meta.u_output_generate_query(o.output_id);

	WITH w AS (
		SELECT o.output_id, ROW_NUMBER() OVER (PARTITION BY o.file_name ORDER BY o.output_id) rn
	    FROM _outputs o
	)
	UPDATE _outputs o
	SET file_name = file_name || '_' || w.rn
	FROM w
	WHERE w.output_id = o.output_id AND w.rn > 1;

	SELECT json_agg(json_build_object('file_name', file_name || '.sql', 'query', query))
	INTO v_output_queries
	FROM _outputs;

	SELECT string_agg(query,E'\n\n' ORDER BY level)
	INTO v_all_output_query
	FROM _outputs;

	RETURN json_build_object('source', v_source_queries, 'output',v_output_queries, 'run', E'/*SOURCES*/\n' || v_all_source_query || COALESCE( E'\n/*OUTPUTs*/\n' || v_all_output_query, ''));


END;

$function$;