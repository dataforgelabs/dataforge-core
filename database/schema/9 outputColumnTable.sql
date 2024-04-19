CREATE TABLE IF NOT EXISTS meta.output_column
(
    output_column_id integer NOT NULL DEFAULT nextval('meta.seq_output_column'::regclass),
    output_id integer NOT NULL,
    position integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    datatype text COLLATE pg_catalog."default" DEFAULT 'text'::text,
    precision smallint,
    scale smallint,
    max_length smallint,
    created_userid text COLLATE pg_catalog."default" NOT NULL DEFAULT "current_user"(),
    create_datetime timestamp without time zone NOT NULL DEFAULT now(),
    updated_userid text COLLATE pg_catalog."default",
    update_datetime timestamp without time zone,
    description text COLLATE pg_catalog."default",
    partition_ordinal integer,
    zorder_ordinal integer,
    CONSTRAINT pk_output_column PRIMARY KEY (output_column_id),
    CONSTRAINT fk_output_column_output_id FOREIGN KEY (output_id)
        REFERENCES meta.output (output_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT ck_output_column_precision CHECK ("precision" >= 0),
    CONSTRAINT ck_output_column_scale CHECK (scale >= 0),
    CONSTRAINT ux_output_column_name UNIQUE (output_id, name)
);

CREATE INDEX IF NOT EXISTS ux_output_column_output_id_position
    ON meta.output_column USING btree
    (output_id, "position")
    TABLESPACE pg_default;
