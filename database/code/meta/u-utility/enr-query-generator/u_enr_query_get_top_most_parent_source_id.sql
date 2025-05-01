CREATE OR REPLACE FUNCTION meta.u_enr_query_get_top_most_parent_sub_source_enrichment(in_source meta.source)
 RETURNS meta.enrichment
 LANGUAGE plpgsql
 
AS $function$
DECLARE
	v_source meta.source;

BEGIN

	PERFORM meta.u_assert(in_source.connection_type = 'sub_source', format('Source %s is not a sub-source', to_json(in_source)));
	PERFORM meta.u_assert(in_source.sub_source_enrichment_id IS NOT NULL, format('Sub-source %s sub_source_enrichment_id is NULL', to_json(in_source)));

	SELECT s.* INTO v_source
	FROM meta.enrichment e 
	JOIN meta.source s ON s.source_id = e.source_id
	WHERE e.enrichment_id = in_source.sub_source_enrichment_id;

	IF v_source.connection_type = 'sub_source' THEN
		-- nested sub-source
		RETURN meta.u_enr_query_get_top_most_parent_sub_source_enrichment(v_source);
	ELSE
		RETURN (
			SELECT e FROM meta.enrichment e
			WHERE e.enrichment_id = in_source.sub_source_enrichment_id
		);
	END IF;

END;

$function$;