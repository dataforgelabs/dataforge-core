
CREATE OR REPLACE FUNCTION meta.u_get_source_name(in_source_id int, templatize_flag boolean = false)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_source_template_id int;
	v_source_name text;

BEGIN
	SELECT s.source_name, s.source_template_id
	INTO v_source_name, v_source_template_id 
	FROM meta.source s WHERE s.source_id = in_source_id;

	IF v_source_name IS NULL THEN
		RETURN format('Unknown source_id=%s', in_source_id);
	END IF;

	IF NOT templatize_flag OR v_source_template_id IS NULL THEN
		RETURN v_source_name;
	END IF;

	RETURN COALESCE(
		(SELECT object_name FROM meta.object_template t WHERE t.object_template_id = v_source_template_id),
		format('Unknown source_template_id=%s', v_source_template_id));

END;

$function$;
