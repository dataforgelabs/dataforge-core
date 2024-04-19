
CREATE TABLE IF NOT EXISTS meta.import_object
(
    import_object_id integer NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    import_id integer NOT NULL,
    file_path text, -- sources, outputs, ..
    object_type text, -- source/output/group/...
    name text, -- object name
    hash text, -- md5 hash of file
    id int, -- object_id, e.g. source_id
    body_text text, -- text of imported json file
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



