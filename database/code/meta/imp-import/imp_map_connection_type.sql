CREATE OR REPLACE FUNCTION meta.imp_map_connection_type(in_body jsonb)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_connection_type text = in_body->>'connection_type';

BEGIN
    RETURN CASE v_connection_type WHEN 'custom' THEN in_body || jsonb_build_object('connection_type','custom_ingestion') ELSE in_body END;
END;
$function$;