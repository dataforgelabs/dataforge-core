CREATE OR REPLACE FUNCTION meta.u_get_source_project(in_source_id int)
 RETURNS int
 LANGUAGE plpgsql
 COST 10
AS $function$

BEGIN

RETURN (SELECT project_id FROM meta.source WHERE source_id = in_source_id);
END;

$function$;
