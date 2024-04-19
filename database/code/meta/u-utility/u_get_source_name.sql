
CREATE OR REPLACE FUNCTION meta.u_get_source_name(in_source_id int)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

BEGIN
	
RETURN COALESCE(
	(SELECT s.source_name FROM meta.source s WHERE s.source_id = in_source_id),
	format('Unknown source_id=%s', in_source_id));

END;

$function$;
