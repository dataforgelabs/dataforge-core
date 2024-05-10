CREATE OR REPLACE FUNCTION meta.imp_parse_objects(in_import_id int)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_stack text;
    v_log_id int;
BEGIN
    SELECT log_id INTO v_log_id FROM meta.import WHERE import_id = in_import_id;
    -- parse files and extract names
    WITH parsed AS (
        SELECT import_object_id, body_text::jsonb body
        FROM meta.import_object WHERE import_id = in_import_id)
    UPDATE meta.import_object io
    SET body = p.body, 
        name =  CASE WHEN object_type IN ('source' ,'output', 'group', 'token') THEN p.body->>(object_type || '_name')
                WHEN object_type IN ('output_template','source_template') THEN p.body->>'object_name'
        ELSE p.body->>'name' END
    FROM parsed p WHERE p.import_object_id = io.import_object_id;

    INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
    VALUES ( v_log_id, 'Import files parsing completed','imp_parse_objects', 'I', clock_timestamp());
END;
$function$;