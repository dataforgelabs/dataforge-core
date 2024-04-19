CREATE OR REPLACE  FUNCTION meta.u_get_relation_label(in_source_relation_id int, in_reverse_flag boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$

BEGIN

RETURN (
	SELECT jsonb_build_object('label',  '[' || CASE WHEN in_reverse_flag THEN rs.source_name ELSE s.source_name END || E'] \u2192 ' || sr.relation_name || E' \u2192 [' || CASE WHEN in_reverse_flag THEN s.source_name ELSE rs.source_name END || ']',
	'expression',
	CASE WHEN sr.source_id <> sr.related_source_id THEN replace(replace(sr.expression, '[This]', '[' || s.source_name || ']'), '[Related]', '[' || rs.source_name || ']')
	ELSE replace(sr.expression, '[This]', '[' || s.source_name  || ']') 
	END )
	FROM meta.source_relation sr
	JOIN meta.source s ON s.source_id = sr.source_id
	JOIN meta.source rs ON rs.source_id = sr.related_source_id
	WHERE sr.source_relation_id = in_source_relation_id
);
	
	
END;

$function$;

