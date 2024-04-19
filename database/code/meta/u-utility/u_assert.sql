CREATE OR REPLACE FUNCTION meta.u_assert(in_assert_boolean boolean, in_message text)
 RETURNS void
 LANGUAGE plpgsql
 COST 100
AS $function$
BEGIN
    IF NOT COALESCE(in_assert_boolean,false) THEN
        RAISE EXCEPTION '%', in_message;
    END IF;
END;
$function$;
