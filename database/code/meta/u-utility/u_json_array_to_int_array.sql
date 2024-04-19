CREATE OR REPLACE FUNCTION meta.u_json_array_to_int_array(in_parameters json)
 RETURNS int[]
 LANGUAGE plpgsql
AS $function$

DECLARE
v_ret int[];
BEGIN
    IF in_parameters::jsonb IS DISTINCT FROM 'null'::jsonb THEN
        SELECT array_agg(t::int) INTO v_ret FROM json_array_elements_text(in_parameters) t;
        RETURN v_ret;
    END IF;

    RETURN null;

END;
$function$;

