CREATE OR REPLACE FUNCTION meta.svcc_get_output_queries(in_import_id int)
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
        SELECT o.output_id, lower(regexp_replace(left(o.output_name,245),'["<>:\/\\|?*]','_','g')) name_reg
        FROM meta.output o WHERE o.project_id = v_imp.project_id
    ),
	 rwn AS (
		SELECT r.output_id, r.name_reg, ROW_NUMBER() OVER (PARTITION BY r.name_reg ORDER BY r.output_id) rn
	    FROM rgx r
	)
	SELECT json_agg(json_build_object('file_name', CASE WHEN w.rn > 1 THEN w.name_reg || '_' || w.rn ELSE w.name_reg END || '.sql', 
	 'query', meta.u_output_generate_query(w.output_id) ))
	INTO v_ret
	FROM rwn w;


	RETURN v_ret;


END;

$function$;