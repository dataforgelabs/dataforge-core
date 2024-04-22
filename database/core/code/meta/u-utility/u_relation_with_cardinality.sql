-- returns next relations chainable to in_source_id
CREATE OR REPLACE FUNCTION meta.u_relation_with_cardinality(in_source_id int)
 RETURNS TABLE (source_relation_id int, relation_template_id int, reverse_flag boolean, cardinality text, one_to_one_flag boolean, next_source_id int)
 LANGUAGE plpgsql
AS $function$

BEGIN

RETURN QUERY
SELECT sr.source_relation_id, sr.relation_template_id, 
CASE WHEN sr.source_id = sr.related_source_id OR sr.source_id = in_source_id THEN false ELSE true END reverse_flag, 
CASE WHEN sr.source_id = sr.related_source_id OR sr.source_id = in_source_id THEN sr.related_source_cardinality 
	ELSE sr.source_cardinality END cardinality, 
sr.source_cardinality = '1' AND sr.related_source_cardinality = '1' one_to_one_flag, 
CASE WHEN sr.source_id = in_source_id THEN sr.related_source_id ELSE sr.source_id END next_source_id
FROM meta.source_relation sr 
WHERE in_source_id IN (sr.source_id, sr.related_source_id) AND sr.active_flag;
	
END;

$function$;

