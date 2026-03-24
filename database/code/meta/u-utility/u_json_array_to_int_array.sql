CREATE OR REPLACE FUNCTION meta.u_json_array_to_int_array(in_parameters json)
 RETURNS int[]
 LANGUAGE sql
AS $function$
    SELECT CASE
        WHEN in_parameters::jsonb IS DISTINCT FROM 'null'::jsonb THEN (
            SELECT array_agg(t::int) FROM json_array_elements_text(in_parameters) t
        )
        ELSE NULL::int[]
    END;
$function$;

