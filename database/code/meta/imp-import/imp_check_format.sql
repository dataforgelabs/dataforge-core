CREATE OR REPLACE FUNCTION meta.imp_check_format(in_import_id int, in_format text)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$
DECLARE
	v_major int;
    v_minor int;
    v_format_text_arr text[];
    v_format_spec text;
    v_current_major int := 2;
    v_current_minor int := 1;
BEGIN

    v_format_text_arr := regexp_match(in_format, '(^|core)(\d+)\.(\d+)$');
    v_format_spec = v_format_text_arr[1]; --  'core'
    v_major = v_format_text_arr[2]::int;
    v_minor = v_format_text_arr[3]::int;

    IF v_major IS NULL OR v_minor IS NULL OR v_format_spec NOT IN ('core') THEN
        RAISE EXCEPTION 'Invalid import format %', in_format;
    END IF;

        IF v_major <> 1 OR v_minor <> 0 THEN
            RAISE EXCEPTION 'Unsupported core format %', in_format;
        END IF;

    UPDATE meta.import SET  format = format('%s.%s',v_major, v_minor), 
                            format_spec = v_format_spec 
    WHERE import_id = in_import_id;

END;
$function$;