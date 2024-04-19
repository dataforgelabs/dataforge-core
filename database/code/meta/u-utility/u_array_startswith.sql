CREATE OR REPLACE FUNCTION meta.u_array_starts_with(in_test int[], in_starts_with int[])
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_length_startswith int = cardinality(in_starts_with);
    v_length_test int = cardinality(in_test);
    v_index int;
BEGIN

IF v_length_startswith = 0 THEN
    RETURN true;
ELSEIF v_length_startswith > v_length_test THEN
    RETURN false;
END IF;

FOR v_index IN 1..v_length_startswith LOOP
    IF in_test[v_index] <> in_starts_with[v_index] THEN 
        RETURN false;
    END IF;
END LOOP;

RETURN true;



END;
$function$;