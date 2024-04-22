CREATE OR REPLACE FUNCTION meta.svc_import_execute(in_import_id int, in_force_flag boolean = false)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_imp meta.import;
    v_err jsonb;
    v_test_flag boolean = false;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

    IF v_imp.format IS NULL THEN
        PERFORM meta.svc_import_complete(in_import_id, 'F', 'Blank format or missing meta.yaml');
        RETURN false;
    END IF;

    v_err := meta.impc_execute(v_imp);
    IF v_err IS NOT NULL THEN
        PERFORM meta.svc_import_complete(in_import_id, 'F', v_err::text);
        RETURN false;
    END IF;
    v_test_flag := true;

    RETURN true;
    END;
$function$;
