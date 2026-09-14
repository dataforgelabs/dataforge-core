
CREATE TABLE IF NOT EXISTS meta.import_object
(
    import_object_id integer NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    import_id integer NOT NULL,
    file_path text, -- sources, outputs, ..
    object_type text, -- source/output/group/...
    name text, -- object name
    hash text, -- md5 hash of file
    id int, -- object_id, e.g. source_id
    body jsonb, -- parsed jsonb json file,
    changed_flag boolean NOT NULL DEFAULT false, -- True if object has changed vs. existing
    new_flag boolean, -- true if object was added by import
    rules_changed_flag boolean,
    channels_changed_flag boolean,
    raw_attributes_changed_flag boolean,
    tokens_changed_flag boolean,
    relations_changed_flag boolean,
    dependencies_changed_flag boolean,
    output_columns_changed_flag boolean,
    CONSTRAINT ux_import_object_name UNIQUE (import_id,object_type,name),
    CONSTRAINT ux_import_object_id UNIQUE (import_id,object_type,id),
    CONSTRAINT fk_import_object_import FOREIGN KEY (import_id) REFERENCES meta.import (import_id) ON DELETE CASCADE
);



-- Preserve prepared imports; fail imports whose payloads were never parsed.
-- On the old schema, parsing populated every body in one atomic statement.
DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'meta' AND table_name = 'import_object'
                 AND column_name = 'body_text') THEN
        UPDATE meta.import i
        SET parameters = COALESCE(i.parameters, '{}'::jsonb)
                         || '{"import_prepared_flag":true}'::jsonb
        WHERE i.type IN ('import', 'pull', 'init-pull', 'push')
          AND EXISTS (SELECT 1 FROM meta.import_object io WHERE io.import_id = i.import_id)
          AND NOT EXISTS (SELECT 1 FROM meta.import_object io
                          WHERE io.import_id = i.import_id AND io.body IS NULL);

        WITH failed AS (
            UPDATE meta.import i
            SET status_code = 'F',
                parameters = COALESCE(i.parameters, '{}'::jsonb)
                             || '{"import_prepared_flag":false}'::jsonb
            WHERE EXISTS (SELECT 1 FROM meta.import_object io
                          WHERE io.import_id = i.import_id AND io.body IS NULL)
            RETURNING i.log_id
        )
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        SELECT log_id, 'Import failed: unparsed import data removed during upgrade. Start a new import or Git operation.',
               'import_object_migration', 'E', clock_timestamp()
        FROM failed;

        ALTER TABLE meta.import_object DROP COLUMN body_text;
    END IF;
END;
$migration$;
