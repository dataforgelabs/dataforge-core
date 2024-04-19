CREATE OR REPLACE FUNCTION meta.svcc_get_source_queries(in_import_id int)
 RETURNS json 
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_imp meta.import;
    v_ret json;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

	-- clean file names
	WITH rgx AS (
        SELECT s.source_id, lower(regexp_replace(left(s.source_name,245),'["<>:\/\\|?*]','_','g')) name_reg
        FROM meta.source s WHERE s.project_id = v_imp.project_id
    ),
	 rwn AS (
		SELECT r.source_id, r.name_reg, ROW_NUMBER() OVER (PARTITION BY r.name_reg ORDER BY r.source_id) rn
	    FROM rgx r
	)
	SELECT json_agg(json_build_object('file_name', CASE WHEN w.rn > 1 THEN w.name_reg || '_' || w.rn ELSE w.name_reg END || '.sql', 
	 'query', meta.u_enr_query_generate_query(w.source_id) ))
	INTO v_ret
	FROM rwn w;


	RETURN v_ret;


END;

$function$;