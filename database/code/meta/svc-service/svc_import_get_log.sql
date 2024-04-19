CREATE OR REPLACE FUNCTION meta.svc_import_get_log(in_import_id int)
    RETURNS text
    LANGUAGE plpgsql
AS
$function$

BEGIN
    RETURN (
        SELECT string_agg(ln, E'\n')
            FROM ( SELECT format(E'%s\t%s\t%s', l.insert_datetime, CASE l.severity WHEN 'I' THEN 'INFO' WHEN 'W' THEN 'WARN' WHEN 'E' THEN 'ERROR' END, l.message) ln
                FROM log.actor_log l
                JOIN meta.import i ON i.log_id = l.log_id AND i.import_id = in_import_id
                ORDER BY l.insert_datetime
            ) t
    );

END;
$function$;
