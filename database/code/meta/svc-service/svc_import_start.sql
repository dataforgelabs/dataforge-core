CREATE OR REPLACE FUNCTION meta.svc_import_start()
    RETURNS int
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_import_id int;
    v_project_id int;
BEGIN
    v_project_id := (SELECT project_id FROM meta.project WHERE default_flag);

        INSERT INTO meta.import(  project_id, type, status_code, created_userid ) VALUES
        ( v_project_id, 'import', 'I', 'system')
        RETURNING import_id INTO v_import_id;

        RETURN v_import_id;
END;
$function$;
