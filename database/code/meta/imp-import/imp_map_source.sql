CREATE OR REPLACE FUNCTION meta.imp_map_source(in_name text, in_project_id int, in_throw_error boolean = true)
    RETURNS int
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_ret int;
BEGIN
IF in_name IS NULL THEN
	RETURN null;
ELSE
	SELECT source_id INTO v_ret FROM meta.source WHERE source_name = in_name AND project_id = in_project_id;
    PERFORM meta.u_assert(NOT in_throw_error OR v_ret IS NOT NULL,'Source ' || in_name || ' does not exist in project ' || COALESCE(in_project_id::text,'NULL'));
	RETURN v_ret;
END IF;
END;
$function$;