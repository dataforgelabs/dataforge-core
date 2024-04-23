
CREATE OR REPLACE FUNCTION meta.u_get_source_table_name(in_source_id int)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

BEGIN
	
RETURN COALESCE(
		(SELECT s.ingestion_parameters->>'source_table' 
			FROM meta.source s WHERE s.source_id = in_source_id),
		format('Undefined source table for source `%s`', meta.u_get_source_name(in_source_id))
	);

END;

$function$;
