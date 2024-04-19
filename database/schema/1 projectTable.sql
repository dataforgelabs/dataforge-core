CREATE TABLE IF NOT EXISTS meta.project
(
    project_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    name text NOT NULL,
    description text,
    default_flag boolean NOT NULL DEFAULT (false),
    max_imports int NOT NULL DEFAULT 50,
    schema_name text NOT NULL,
    disable_ingestion_flag boolean DEFAULT false,
    lock_flag boolean DEFAULT false,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    created_userid text,
    update_datetime timestamp without time zone,
    updated_userid text,
    CONSTRAINT pk_project PRIMARY KEY (project_id),
    CONSTRAINT ux_project_name UNIQUE (name),
    CONSTRAINT ux_project_schema_name UNIQUE (schema_name)
);

DO $$
DECLARE
    v_schema text;
BEGIN
IF NOT EXISTS(SELECT 1 FROM meta.project) THEN -- insert default project record with project_id = 1
    INSERT INTO meta.project (name, description, default_flag,created_userid, schema_name)
      VALUES('Default', 'Default project automatically created by system', true, 'system', 'default');
END IF;   
END;
$$;  

