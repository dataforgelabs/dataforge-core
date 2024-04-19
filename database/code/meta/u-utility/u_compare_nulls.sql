CREATE OR REPLACE FUNCTION meta.u_compare_nulls(in_arg1 anyelement, in_arg2 anyelement)
    RETURNS boolean
    IMMUTABLE
    LANGUAGE plpgsql
AS
$function$

BEGIN

RETURN (in_arg1 IS NULL AND in_arg2 IS NULL) OR (in_arg1 IS NOT NULL AND in_arg2 IS NOT NULL);
    
END;
$function$;

