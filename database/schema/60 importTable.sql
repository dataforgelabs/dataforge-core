
CREATE TABLE IF NOT EXISTS meta.import
(
    import_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    project_id int NOT NULL,
    type text NOT NULL DEFAULT 'import', 
    status_code char(1),
    log_id int NOT NULL DEFAULT nextval('log.seq_log'::regclass),
    parameters jsonb,
    file_name text,
    format text,
    format_spec text,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    created_userid text COLLATE pg_catalog."default",
    CONSTRAINT pk_import PRIMARY KEY (import_id),
    CONSTRAINT fk_import_project FOREIGN KEY (project_id) REFERENCES meta.project (project_id)
);

