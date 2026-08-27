CREATE OR REPLACE FUNCTION meta.svc_import_execute(in_import_id int, in_force_flag boolean = false)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$function$
DECLARE
    v_imp meta.import;
BEGIN
    SELECT * INTO v_imp FROM meta.import WHERE import_id = in_import_id;

    IF v_imp.format IS NULL THEN
        PERFORM meta.svc_import_complete(in_import_id, 'F', 'Blank format or missing meta.yaml');
        RETURN false;
    END IF;

    PERFORM meta.svc_import_complete(
        in_import_id,
        'F',
        'Import format_spec=''core'' is temporarily unsupported. Use the standard import format; asynchronous core-format expression testing is tracked in DEV-5751.'
    );
    RETURN false;
    END;
$function$;
