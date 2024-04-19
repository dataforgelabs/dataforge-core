
DROP FUNCTION IF EXISTS meta.u_validate_output(in_output meta.output, in_import_mode text, OUT out_status character, OUT out_error text);
CREATE OR REPLACE FUNCTION meta.u_validate_output(in_output meta.output)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_output_id int;
    v_error json;
    v_original meta.output;
    v_source_names text[];
    v_bad_columns text;

BEGIN

    SELECT *
    INTO v_original
    FROM meta.output o
    WHERE output_id = in_output.output_id;

IF in_output.output_type = 'table' AND in_output.active_flag THEN
    SELECT json_build_object('output_name', o.output_name, 'project_name', p.name, 
    'table_name', in_output.output_package_parameters->>'table_name', 'table_schema',in_output.output_package_parameters->>'table_schema') 
    INTO v_error
    FROM meta.output o
    JOIN meta.project p ON o.project_id = p.project_id
    WHERE o.output_sub_type = in_output.output_sub_type
    AND o.output_type = in_output.output_type
    AND o.connection_id = in_output.connection_id
    AND output_package_parameters->>'table_name' = in_output.output_package_parameters->>'table_name'
    AND output_package_parameters->>'table_schema' = in_output.output_package_parameters->>'table_schema'
    AND output_id IS DISTINCT FROM in_output.output_id
    AND o.active_flag;
    IF v_error IS NOT NULL THEN
        RETURN format('An output writing to table already exists: %s',v_error);
    END IF;
END IF;

IF in_output.output_sub_type = 'text' AND (SELECT count(1) FROM meta.output_column WHERE output_id = in_output.output_id) > 1 THEN
     RETURN 'Text outputs can only have a single output column! Please remove excess columns or choose another output file type.';
END IF;

IF (in_output.output_type = 'file' AND (in_output.output_sub_type = 'parquet' OR in_output.output_sub_type = 'avro')) OR (in_output.output_type = 'table' AND in_output.output_sub_type = 'delta_lake') OR in_output.output_type = 'table'
    THEN
        SELECT string_agg(oc.name,',') INTO v_bad_columns
        FROM meta.output_column oc WHERE in_output.output_id = oc.output_id AND oc.name !~ ('^[a-zA-Z_]+[a-zA-Z0-9_]*$');
        IF v_bad_columns IS NOT NULL THEN
            IF in_output.output_type = 'table' THEN
                RETURN 'Output table type, column name must start with a letter and may contain letters, numbers, _ or spaces. column names: ' || v_bad_columns;
            ELSE
                RETURN 'Output types parquet, avro and delta lake cannot have spaces or special symbols in the column names: ' || v_bad_columns;
            END IF;
        END IF;
END IF;

IF in_output.output_type = 'virtual' AND in_output.output_package_parameters->>'view_name' IS NULL THEN
      RETURN 'Could not update Output ' || in_output.output_name || ' with a blank view name.';
END IF;

IF in_output.output_type = 'virtual' AND in_output.active_flag THEN
    SELECT json_build_object('output_name', o.output_name, 'project_name', p.name,
        'view_name',in_output.output_package_parameters->>'view_name', 'view_database', COALESCE(in_output.output_package_parameters->>'view_database',ip.schema_name) ) 
    INTO v_error
    FROM meta.output o
    JOIN meta.project p ON o.project_id = p.project_id
    JOIN meta.project ip ON in_output.project_id = ip.project_id
    WHERE o.output_type = in_output.output_type
    AND o.output_package_parameters->>'view_name' = in_output.output_package_parameters->>'view_name'
    AND COALESCE(o.output_package_parameters->>'view_database',p.schema_name) = COALESCE(in_output.output_package_parameters->>'view_database',ip.schema_name)
    AND output_id IS DISTINCT FROM in_output.output_id
    AND o.active_flag;

    IF v_error IS NOT NULL THEN
        RETURN format('An output writing to view already exists: %s',v_error);
    END IF;
END IF;


IF in_output.output_type <> 'virtual' AND v_original.output_type = 'virtual' THEN
    SELECT array_agg(s.source_name)
       INTO v_source_names
       FROM meta.source s WHERE s.loopback_output_id = in_output.output_id;
    IF v_source_names IS NOT NULL THEN
        RETURN 'Virtual output is referenced by loopback sources ' || v_source_names::text;
    END IF;
END IF;

RETURN '';

END;

$function$;


CREATE OR REPLACE FUNCTION meta.u_validate_output(in_output json)
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_o meta.output;
BEGIN

SELECT * FROM json_populate_record(null::meta.output, in_output )  INTO v_o;

RETURN meta.u_validate_output(v_o);

END;

$function$;