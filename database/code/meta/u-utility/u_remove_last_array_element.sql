CREATE OR REPLACE FUNCTION meta.u_remove_last_array_element(
    in_ids int[])
 RETURNS int[] -- returns array with last element removed
 LANGUAGE plpgsql
 COST 1
 IMMUTABLE PARALLEL SAFE
AS $function$
DECLARE
    i int;
    v_ret  int[] := '{}';
BEGIN

FOR i IN 1..array_upper(in_ids, 1) - 1 LOOP
    v_ret := v_ret || in_ids[i];
END LOOP;

RETURN v_ret;
END;

$function$;