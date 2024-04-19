CREATE OR REPLACE FUNCTION meta.u_get_hub_table_name(in_source_id int, in_history_flag boolean = false)
 RETURNS text 
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_table text;
	v_source meta.source;
BEGIN
	SELECT s.* INTO v_source FROM meta.source s WHERE s.source_id = in_source_id;

	IF v_source.refresh_type = 'unmanaged' THEN	
		RETURN v_source.ingestion_parameters->>'table_name';
	ELSE
		RETURN v_source.hub_view_name;
	END IF;
END;

$function$;