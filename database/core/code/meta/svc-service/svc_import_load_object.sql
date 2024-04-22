CREATE OR REPLACE FUNCTION meta.svc_import_load_object(in_import_id int, in_path text, in_body text)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_object_type text;
    v_format text;
    v_obj_types text[] = ARRAY['source','output'];
BEGIN
    -- check format
    IF in_path = 'meta.yaml' THEN
        PERFORM meta.imp_check_format(in_import_id, (in_body::json)->>'format');
    ELSEIF in_path = 'variables.yaml' THEN
        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, 'variables', in_body);
    ELSEIF in_path = 'relations.yaml' THEN
        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, 'relations', in_body);
    ELSEIF in_path = 'defaults.yaml' THEN
        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, 'defaults', in_body);
    ELSEIF in_path ~ format('^(%s)s/', array_to_string(v_obj_types,'|')) THEN
        v_object_type := substring(in_path from '^(\w+)s/');

        IF v_object_type IS NULL THEN
            RAISE EXCEPTION 'Unable to parse import object type, invalid path %', in_path;
        END IF;

        IF v_object_type NOT IN ('source','output', 'group', 'token','output_template','source_template','rule_template','relation_template') THEN
                RAISE EXCEPTION 'Unknown object type % in file %', v_object_type, in_path;
        END IF;

        INSERT INTO meta.import_object(  import_id, file_path, object_type, body_text) VALUES
        (in_import_id, in_path, v_object_type, in_body);
    ELSE
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        SELECT i.log_id, format('Skipped unknown object %s. Please check you project',in_path), 'svc_import_restart', 'W', now()
        FROM meta.import i WHERE i.import_id = in_import_id;
    END IF;

END;
$function$;
