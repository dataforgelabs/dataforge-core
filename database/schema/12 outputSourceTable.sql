CREATE TABLE IF NOT EXISTS meta.output_source
(
    output_source_id integer NOT NULL DEFAULT nextval('meta.seq_output_source'::regclass),
    source_id integer NOT NULL,
    output_id integer NOT NULL,
    filter text COLLATE pg_catalog."default",
    operation_type text COLLATE pg_catalog."default",
    unpivot_list text[] COLLATE pg_catalog."default",
    union_output_position integer,
    active_flag boolean NOT NULL,
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    updated_userid text COLLATE pg_catalog."default",
    update_datetime timestamp without time zone,
    output_package_parameters jsonb,
    output_source_name text COLLATE pg_catalog."default" NOT NULL,
    include_pass_flag boolean,
    include_fail_flag boolean,
    include_warn_flag boolean,
    description text,
    ipu_weight int,
    CONSTRAINT pk_output_source PRIMARY KEY (output_source_id),
    CONSTRAINT output_source_source_name UNIQUE (output_id, source_id, output_source_name),
    CONSTRAINT fk_output_source_output_id FOREIGN KEY (output_id)
        REFERENCES meta.output (output_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_output_source_source FOREIGN KEY (source_id)
        REFERENCES meta.source (source_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS "IX_output_source_output_id"
    ON meta.output_source USING btree
    (output_id)
    TABLESPACE pg_default;




