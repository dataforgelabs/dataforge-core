CREATE OR REPLACE FUNCTION meta.u_enr_query_relation_alias(
    in_ids int[])
 RETURNS text -- returns array with last element removed
 LANGUAGE plpgsql
 COST 1
 IMMUTABLE PARALLEL SAFE
AS $function$
BEGIN


RETURN (
	SELECT string_agg( el::text, '_')
	FROM unnest(in_ids) el
	);
END;

$function$;