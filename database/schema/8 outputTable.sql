CREATE TABLE IF NOT EXISTS meta.output
(
    output_id integer NOT NULL DEFAULT nextval('meta.seq_output'::regclass),
    output_type text COLLATE pg_catalog."default" NOT NULL,
    output_name text COLLATE pg_catalog."default" NOT NULL,
    active_flag boolean NOT NULL,
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    update_datetime timestamp without time zone,
    output_package_parameters jsonb,
    updated_userid text COLLATE pg_catalog."default",
    retention_parameters jsonb,
    output_description text COLLATE pg_catalog."default",
    output_sub_type text COLLATE pg_catalog."default",
    connection_id int,
    alert_parameters jsonb,
    post_output_type text,
    custom_post_output_cluster_configuration_id int,
    group_id int,
    output_template_id int,
    project_id int NOT NULL DEFAULT 1,
    CONSTRAINT pk_output PRIMARY KEY (output_id),
    CONSTRAINT ux_output_project_output_name UNIQUE (project_id, output_name),
    CONSTRAINT fk_output_project FOREIGN KEY (project_id) REFERENCES meta.project (project_id)
);




