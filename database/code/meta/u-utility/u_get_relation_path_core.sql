-- return relation path from _impc_enrichment_parameter table
CREATE OR REPLACE FUNCTION meta.u_get_relation_path_core(in_enrichment_id int, in_source_name text)
 RETURNS int[]
 LANGUAGE plpgsql
AS $function$

BEGIN

IF NOT EXISTS(SELECT 1 FROM pg_tables where tablename = '_impc_enrichment_parameter') THEN
	RETURN null;
END IF;

RETURN (
	SELECT source_relation_ids
	FROM _impc_enrichment_parameter p
	WHERE p.enrichment_id = in_enrichment_id AND
	COALESCE(p.source_name,in_source_name) = in_source_name
);
	
END;

$function$;

