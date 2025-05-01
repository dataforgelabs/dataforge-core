CREATE OR REPLACE FUNCTION meta.svc_import_complete(in_import_id int, in_status_code char(1), in_err text = null)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_import_id int;
    v_log_id int;
BEGIN
    UPDATE meta.import SET status_code = in_status_code
    WHERE import_id = in_import_id
    RETURNING log_id INTO v_log_id;

    IF in_status_code = 'F' THEN
       INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, 'Import failed : ' || COALESCE(in_err, 'NULL'),'svc_import_complete', 'E', clock_timestamp());
    ELSEIF in_status_code = 'Q' THEN
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, 'Import loaded. ' || COALESCE(in_err, ''),'svc_import_complete', 'I', clock_timestamp());
    ELSEIF in_status_code = 'D' THEN
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, in_err || ' Click Restart to proceed with import','svc_import_complete', 'W', clock_timestamp() + interval '1 second'); -- makes it appear last
    ELSE 
       INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( v_log_id, 'Import complete. ' || COALESCE(in_err, ''),'svc_import_complete', 'I', clock_timestamp());
    END IF;        
END;
$function$;
