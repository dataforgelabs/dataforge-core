CREATE TABLE IF NOT EXISTS meta.source
(
    source_id integer NOT NULL DEFAULT nextval('meta.seq_source'::regclass),
    source_name text COLLATE pg_catalog."default" NOT NULL,
    source_description text COLLATE pg_catalog."default" NOT NULL,
    active_flag boolean NOT NULL,
    ignore_failed_validations_flag boolean,
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    ingestion_parameters jsonb,
    created_userid text COLLATE pg_catalog."default",
    update_datetime timestamp without time zone DEFAULT now(),
    parsing_parameters jsonb,
    cdc_refresh_parameters jsonb,
    updated_userid text COLLATE pg_catalog."default",
    schedule_id int,
    alert_parameters jsonb,
    file_type text,
    refresh_type text,
    connection_id int,
    connection_type text,
    initiation_type text,
    loopback_output_id int,
    cost_parameters jsonb,
    parser text,
    source_template_id int,
    group_id int,
    hub_view_name text,
    hub_table_size bigint,
    process_configuration_id int,
    custom_ingest_cluster_configuration_id int,
    custom_parse_cluster_configuration_id int,
    cleanup_configuration_id int,
    project_id int NOT NULL DEFAULT 1,
    processing_type text,
    ipu_rules_weight int,
    sub_source_enrichment_id int,
    CONSTRAINT pk_source PRIMARY KEY (source_id),
    CONSTRAINT ux_source_project_source_name UNIQUE (project_id, source_name),
    CONSTRAINT ux_source_project_hub_view_name UNIQUE (project_id, hub_view_name),
    CONSTRAINT fk_source_project FOREIGN KEY (project_id) REFERENCES meta.project (project_id),
    CONSTRAINT ux_source_sub_source_enrichment_id UNIQUE (sub_source_enrichment_id)
);

