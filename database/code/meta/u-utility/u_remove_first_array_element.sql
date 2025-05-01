CREATE OR REPLACE FUNCTION meta.u_remove_first_array_element(
    in_ids int[])
 RETURNS int[] -- returns array with last element removed
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE
AS $function$
DECLARE
    i int;
    v_ret  int[] := '{}';
BEGIN

FOR i IN 2..array_upper(in_ids, 1) LOOP
    v_ret := v_ret || in_ids[i];
END LOOP;

RETURN v_ret;
END;

$function$;